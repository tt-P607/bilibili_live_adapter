"""B 站直播长连协议的二进制打包/解包。

B 站长连包结构（参见 :mod:`docs.API` § 4.1）：

::

    +--------+--------+----+----+--------+--------+----------+
    | 0..3   | 4..5   | 6..7 | 8..11 | 12..15 | 16..    |
    | pktLen | hdrLen | ver  |  op   |  seq   |  body  |
    +--------+--------+------+-------+--------+----------+
       4         2     2      4       4         pktLen-16

所有字段都是大端（network byte order）。

本模块**只做协议格式转换**，不依赖任何 Neo-MoFox 接口，可以单独跑测试。
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import Iterator
from dataclasses import dataclass


# 包头长度固定 16 字节
HEADER_LEN: int = 16

# struct 格式串：大端 ">" + i (pktLen, 4) + h (hdrLen, 2) + h (ver, 2) +
# i (op, 4) + i (seq, 4)。总长 16 字节。
_HEADER_STRUCT = struct.Struct(">ihhii")


# ── 协议版本 ──────────────────────────────────────
VER_PLAIN: int = 0          # body 为 JSON 文本
VER_HEARTBEAT: int = 1      # 操作码相关无 body（如心跳确认）
VER_ZLIB: int = 2           # body 为 zlib 压缩的多包合并
VER_BROTLI: int = 3         # body 为 brotli 压缩的多包合并

# ── 操作码 ────────────────────────────────────────
OP_HEARTBEAT: int = 2       # client → server，纯包头
OP_HEARTBEAT_REPLY: int = 3 # server → client，body 含在线人数
OP_BUSINESS: int = 5        # server → client，业务推送（弹幕/礼物/SC/上舰）
OP_AUTH: int = 7            # client → server，body = auth_body 字符串
OP_AUTH_REPLY: int = 8      # server → client，body = {"code":0} 表示成功


@dataclass
class Header:
    """解析出来的包头。"""

    packet_len: int
    header_len: int
    ver: int
    op: int
    seq: int


def pack(op: int, body: bytes = b"", *, ver: int = VER_PLAIN, seq: int = 1) -> bytes:
    """把 op + body 打包成 B 站长连协议包。

    Args:
        op: 操作码（见模块顶部 OP_* 常量）
        body: body 数据；纯心跳包传 b""
        ver: 协议版本，client → server 的包基本都用 0
        seq: 序列号，按惯例从 1 开始

    Returns:
        完整的二进制包，包含 16 字节包头 + body
    """

    packet_len = HEADER_LEN + len(body)
    header = _HEADER_STRUCT.pack(packet_len, HEADER_LEN, ver, op, seq)
    return header + body


def unpack_header(buf: bytes) -> Header:
    """解析 16 字节包头，返回 :class:`Header`。

    Raises:
        ValueError: 缓冲区长度不足 16
    """

    if len(buf) < HEADER_LEN:
        raise ValueError(f"buffer too short: {len(buf)} < {HEADER_LEN}")
    pkt_len, hdr_len, ver, op, seq = _HEADER_STRUCT.unpack_from(buf, 0)
    return Header(
        packet_len=int(pkt_len),
        header_len=int(hdr_len),
        ver=int(ver),
        op=int(op),
        seq=int(seq),
    )


def unpack(buf: bytes) -> tuple[Header, bytes]:
    """从一个完整包里切出 (header, body)。

    Args:
        buf: 长度至少为 packet_len 的字节缓冲；多余字节会被丢弃

    Returns:
        (header, body) 二元组

    Raises:
        ValueError: buf 不够包头长度，或包头声明的总长 > buf 长度
    """

    header = unpack_header(buf)
    if header.packet_len > len(buf):
        raise ValueError(
            f"packet truncated: declared {header.packet_len}, available {len(buf)}"
        )
    body = buf[HEADER_LEN : header.packet_len]
    return header, body


def iter_packets(buf: bytes) -> Iterator[tuple[Header, bytes]]:
    """从一个连续的字节缓冲里**逐个**切出所有完整包。

    用于解压后多包合并的场景：``ver=2`` (zlib) / ``ver=3`` (brotli) 的 body
    解压后是若干个 plain 包拼接。

    遇到不完整包（最后一段长度不足）就停止迭代，**不抛异常**。
    """

    offset = 0
    total = len(buf)
    while offset + HEADER_LEN <= total:
        try:
            header = unpack_header(buf[offset : offset + HEADER_LEN])
        except ValueError:
            return
        if header.packet_len < HEADER_LEN:
            # 包头声明长度小于包头自己——明显错误，停止解析。
            return
        end = offset + header.packet_len
        if end > total:
            # 不完整包，停止。
            return
        body = buf[offset + HEADER_LEN : end]
        yield header, body
        offset = end


def decompress(ver: int, body: bytes) -> bytes:
    """把压缩 body 解成 plain 字节流（之后再 :func:`iter_packets` 切包）。

    Args:
        ver: 协议版本，必须是 :data:`VER_ZLIB` 或 :data:`VER_BROTLI`
        body: 压缩后的字节

    Returns:
        解压后的字节流，可直接传给 :func:`iter_packets`

    Raises:
        ValueError: ver 不是已知的压缩版本
        ImportError: ver=3 但环境里没装 brotli
    """

    if ver == VER_ZLIB:
        return zlib.decompress(body)
    if ver == VER_BROTLI:
        try:
            import brotli  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "B 站推送了 brotli 压缩包但环境未安装 brotli；"
                "请检查 manifest.json 的 python_dependencies 是否包含 brotli"
            ) from exc
        return brotli.decompress(body)
    raise ValueError(f"unsupported compression ver: {ver}")


__all__ = [
    "HEADER_LEN",
    "Header",
    "OP_AUTH",
    "OP_AUTH_REPLY",
    "OP_BUSINESS",
    "OP_HEARTBEAT",
    "OP_HEARTBEAT_REPLY",
    "VER_BROTLI",
    "VER_HEARTBEAT",
    "VER_PLAIN",
    "VER_ZLIB",
    "decompress",
    "iter_packets",
    "pack",
    "unpack",
    "unpack_header",
]
