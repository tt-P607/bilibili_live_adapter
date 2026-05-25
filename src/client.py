"""B 站直播长连 WebSocket 客户端：鉴权 + 双心跳 + recv 循环。

职责单一：**只管把长连协议跑稳**，不知道业务事件长什么样。

收到 ``op=5`` 业务包后调用 ``on_event(payload)`` 把控制权交给上层
（即 :mod:`.dispatcher`），由它负责把 cmd 路由到 ``MessageEnvelope``。

设计要点（容易踩的坑）：

1. **两套心跳缺一不可**
   - WS ``op=2`` 心跳（每 20 秒）：服务端拿来判长连活跃。
   - HTTP ``/v2/app/heartbeat``（每 20 秒）：服务端拿来判 ``game_id`` 有效。

   漏掉任何一套都会被踢，且服务端不会立刻提示——通常表现为长连静默
   断开后再也收不到推送。

2. **鉴权要等回包**
   连上 ws 后立刻发 ``op=7``，但**必须**等到 ``op=8`` 回包才能开始
   开心跳；否则可能出现"心跳先于鉴权"导致的 4002。

3. **解压后再切包**
   ``ver=2`` (zlib) / ``ver=3`` (brotli) 的 body 是若干个 plain 包合并；
   解压后用 :func:`proto.iter_packets` 逐个还原。每个内层包再单独看 op。

4. **重连不能复用旧 game_id**
   断线后必须重新调 ``api.start_app()`` 拿新的 ``wss_link`` + ``auth_body``，
   不能假设旧的 game_id 还能用。重连职责留给 :mod:`.plugin`，本类只负责
   "一次会话内的长连维持"。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from src.kernel.concurrency import get_task_manager
from src.kernel.logger import get_logger

from . import proto
from .api import BilibiliApi, StartResponse


logger = get_logger("bilibili_live_adapter.client")


EventCallback = Callable[[dict[str, Any]], Awaitable[None]]
"""``op=5`` 业务包 body 反序列化后的回调签名。"""


class BilibiliClientError(RuntimeError):
    """长连建立 / 鉴权 / 发包过程中的本地错误。"""


class BilibiliClient:
    """单次会话的 B 站长连客户端。

    生命周期：

    .. code-block:: text

        client = BilibiliClient(api, on_event, ...)
        await client.start(start_response)   # 建立 ws，鉴权，跑心跳和 recv
        ...
        await client.stop()                   # 关 ws，停心跳

    重启会话（新 game_id）请重新构造一个实例。
    """

    def __init__(
        self,
        *,
        api: BilibiliApi,
        on_event: EventCallback,
        heartbeat_ws_interval: float = 20.0,
        heartbeat_app_interval: float = 20.0,
        auth_timeout: float = 10.0,
    ) -> None:
        """记录回调与心跳间隔，不在构造时连任何东西。

        Args:
            api: 已经构造好的 :class:`BilibiliApi`，用来跑 HTTP 心跳。
            on_event: ``op=5`` body 反序列化后的回调。
            heartbeat_ws_interval: WS ``op=2`` 心跳间隔（秒）。
            heartbeat_app_interval: HTTP ``/heartbeat`` 间隔（秒）。
            auth_timeout: 鉴权回包超时（秒）。超时视为鉴权失败。
        """

        self._api = api
        self._on_event = on_event
        self._heartbeat_ws_interval = float(heartbeat_ws_interval)
        self._heartbeat_app_interval = float(heartbeat_app_interval)
        self._auth_timeout = float(auth_timeout)

        # 运行时状态
        self._ws: Any | None = None
        self._game_id: str = ""
        self._auth_event: asyncio.Event = asyncio.Event()
        self._auth_ok: bool = False
        self._closed_event: asyncio.Event = asyncio.Event()
        self._stopping: bool = False

        # 后台任务句柄
        self._recv_task_info: Any | None = None
        self._hb_ws_task_info: Any | None = None
        self._hb_app_task_info: Any | None = None

    @property
    def is_connected(self) -> bool:
        """ws 是否处于已连接状态（未 close）。"""

        return self._ws is not None and not getattr(self._ws, "closed", True)

    @property
    def auth_ok(self) -> bool:
        """是否已通过鉴权（``op=8`` code=0）。"""

        return self._auth_ok

    async def wait_closed(self) -> None:
        """等到长连结束（recv loop 退出 / 鉴权失败 / 主动 stop）。"""

        await self._closed_event.wait()

    # ── 启动 / 关闭 ───────────────────────────────────

    async def start(self, start_resp: StartResponse) -> None:
        """建立长连、鉴权、启动心跳与 recv 循环。

        Args:
            start_resp: ``api.start_app()`` 的返回；从中取 wss_link[0] 和
                auth_body，并记录 game_id 给后续 HTTP 心跳用。

        Raises:
            BilibiliClientError: ws 连接失败 / 鉴权超时 / 鉴权 code != 0。
        """

        if not start_resp.wss_links:
            raise BilibiliClientError("StartResponse.wss_links 为空，无法建立长连")

        self._game_id = start_resp.game_id
        self._stopping = False
        self._auth_event.clear()
        self._auth_ok = False
        self._closed_event.clear()

        url = start_resp.wss_links[0]
        logger.info(
            f"连接 B 站长连：{url} game_id={start_resp.game_id} "
            f"主播={start_resp.anchor_uname or '(未知)'} room={start_resp.anchor_room_id or 0}"
        )

        # 延迟导入：避免没装 websockets 时整个模块都 import 失败。
        try:
            from websockets.legacy import client as ws_client  # type: ignore
        except ImportError as exc:
            raise BilibiliClientError(
                "未安装 websockets 库；请检查 manifest.json 的 python_dependencies"
            ) from exc

        try:
            # 关闭 websockets 库自带的 keepalive ping（默认每 20s 发 ping，
            # 25s 超时未收到 pong 就强制关连接）。原因：
            # 1. B 站长连协议**已经有自己的 op=2 心跳**（每 20s 一次），
            #    不需要 ws 层的 ping/pong 也能保活；
            # 2. 主线程被 anima_chatter / KFC 的 LLM 调用阻塞 20s+ 时，
            #    ws ping 发不出去会触发 ``keepalive ping timeout`` 强制断
            #    连，进而触发我们的重连逻辑，1 秒内多次调 /v2/app/start
            #    被 B 站限流（7001 请求冷却期），陷入"断 → 限流 → 等
            #    → 断"的循环。
            # 关掉 ws keepalive 后只依赖应用层 op=2 心跳，主线程偶尔阻塞
            # 不会立刻被踢，重连频率大幅下降。
            self._ws = await ws_client.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=10,
            )
        except Exception as exc:
            raise BilibiliClientError(f"WebSocket 连接失败: {exc}") from exc

        # 注意启动顺序：
        # 1) 先启 recv loop（要靠它读到 op=8 才能确认鉴权成功）；
        # 2) 发 op=7；
        # 3) 等鉴权回包；
        # 4) 启动两条心跳。
        tm = get_task_manager()
        self._recv_task_info = tm.create_task(
            self._recv_loop(),
            name="bilibili_live_adapter.recv",
            daemon=True,
        )

        try:
            await self._send_auth(start_resp.auth_body)
        except Exception:
            await self._safe_close_ws()
            raise

        await self._wait_auth_or_raise()

        self._hb_ws_task_info = tm.create_task(
            self._heartbeat_ws_loop(),
            name="bilibili_live_adapter.heartbeat_ws",
            daemon=True,
        )
        self._hb_app_task_info = tm.create_task(
            self._heartbeat_app_loop(),
            name="bilibili_live_adapter.heartbeat_app",
            daemon=True,
        )
        logger.info("B 站长连鉴权通过，心跳循环已启动")

    async def stop(self) -> None:
        """主动停止：取消心跳 / recv，关 ws。"""

        if self._stopping:
            return
        self._stopping = True

        # 取消心跳任务（不要让它们继续往一个即将关闭的 ws 上发包）。
        tm = get_task_manager()
        for info in (self._hb_ws_task_info, self._hb_app_task_info, self._recv_task_info):
            if info is None:
                continue
            try:
                tm.cancel_task(info.task_id)
            except Exception:
                pass
        self._hb_ws_task_info = None
        self._hb_app_task_info = None
        self._recv_task_info = None

        await self._safe_close_ws()
        self._closed_event.set()
        logger.info("B 站长连已关闭")

    # ── 心跳循环 ──────────────────────────────────────

    async def _heartbeat_ws_loop(self) -> None:
        """每 ``heartbeat_ws_interval`` 秒发一次 ``op=2`` 心跳包。"""

        while not self._stopping:
            try:
                await asyncio.sleep(self._heartbeat_ws_interval)
                if self._stopping or self._ws is None:
                    break
                await self._ws.send(proto.pack(proto.OP_HEARTBEAT))
                logger.debug("WS 心跳已发送")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(f"WS 心跳异常: {exc}")
                break  # 心跳失败时退出，由上层（plugin）触发重连

    async def _heartbeat_app_loop(self) -> None:
        """每 ``heartbeat_app_interval`` 秒调一次 ``/v2/app/heartbeat``。"""

        while not self._stopping:
            try:
                await asyncio.sleep(self._heartbeat_app_interval)
                if self._stopping or not self._game_id:
                    break
                await self._api.app_heartbeat(self._game_id)
                logger.debug(f"应用心跳已发送 game_id={self._game_id}")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                # 应用心跳失败比 WS 心跳更严重——说明 game_id 可能已过期。
                # 仍然只记日志退出循环，由 plugin 决定要不要重连。
                logger.warning(f"应用心跳异常: {exc}")
                break

    # ── 鉴权 ──────────────────────────────────────────

    async def _send_auth(self, auth_body: str) -> None:
        """发送 ``op=7`` 鉴权包。``auth_body`` 是 start_app 拿到的字符串。"""

        if self._ws is None:
            raise BilibiliClientError("ws 尚未建立，无法发送鉴权包")
        # auth_body 是字符串，按文档原样作为 body。
        body = auth_body.encode("utf-8") if isinstance(auth_body, str) else bytes(auth_body)
        await self._ws.send(proto.pack(proto.OP_AUTH, body))
        logger.debug(f"鉴权包已发送 body_size={len(body)}")

    async def _wait_auth_or_raise(self) -> None:
        """等鉴权回包；超时或 code!=0 则抛 :class:`BilibiliClientError`。"""

        try:
            await asyncio.wait_for(self._auth_event.wait(), timeout=self._auth_timeout)
        except asyncio.TimeoutError as exc:
            raise BilibiliClientError(
                f"鉴权回包超时（{self._auth_timeout}s 未收到 op=8）"
            ) from exc

        if not self._auth_ok:
            raise BilibiliClientError("鉴权失败（op=8 返回 code != 0）")

    # ── 接收循环 ──────────────────────────────────────

    async def _recv_loop(self) -> None:
        """持续读 ws 帧，按 op 路由。"""

        assert self._ws is not None
        try:
            async for raw in self._ws:
                if self._stopping:
                    break
                try:
                    if isinstance(raw, str):
                        # B 站长连只发二进制；收到 str 视为异常。
                        logger.warning(f"收到 ws 文本帧（异常）: {raw[:200]}")
                        continue
                    await self._handle_frame(raw)
                except Exception as exc:
                    logger.error(f"处理 ws 帧失败: {exc}", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"recv 循环异常退出: {exc}")
        finally:
            # 让等鉴权的 start() 不会卡死。
            if not self._auth_event.is_set():
                self._auth_event.set()
            self._closed_event.set()
            logger.debug("recv 循环退出")

    async def _handle_frame(self, raw: bytes) -> None:
        """处理一个完整的 ws 帧（可能内含多个压缩后的子包）。"""

        try:
            header, body = proto.unpack(raw)
        except ValueError as exc:
            logger.warning(f"无法解析 ws 帧: {exc}")
            return

        # 压缩包：解压后逐个还原成 plain 包。
        if header.ver in (proto.VER_ZLIB, proto.VER_BROTLI):
            try:
                decompressed = proto.decompress(header.ver, body)
            except Exception as exc:
                logger.error(f"解压 ws body 失败 ver={header.ver}: {exc}", exc_info=True)
                return
            for inner_header, inner_body in proto.iter_packets(decompressed):
                await self._dispatch_packet(inner_header, inner_body)
            return

        await self._dispatch_packet(header, body)

    async def _dispatch_packet(self, header: proto.Header, body: bytes) -> None:
        """按 ``op`` 路由到对应处理。"""

        op = header.op
        if op == proto.OP_AUTH_REPLY:
            self._handle_auth_reply(body)
        elif op == proto.OP_HEARTBEAT_REPLY:
            # 心跳回包 body 包含在线人数，目前不处理。
            logger.debug(f"收到 ws 心跳回包 size={len(body)}")
        elif op == proto.OP_BUSINESS:
            self._handle_business(body)
        else:
            logger.debug(f"忽略未知 op={op} ver={header.ver} size={len(body)}")

    def _handle_auth_reply(self, body: bytes) -> None:
        """``op=8``：解析 ``{"code":0}``，设置 ``_auth_ok`` 并唤醒 ``start()``。"""

        try:
            payload = json.loads(body.decode("utf-8"))
            code = int(payload.get("code", -1))
        except Exception as exc:
            logger.error(f"鉴权回包解析失败: {exc}; body={body[:200]!r}")
            self._auth_ok = False
            self._auth_event.set()
            return

        if code == 0:
            self._auth_ok = True
            logger.info("B 站长连鉴权成功")
        else:
            self._auth_ok = False
            logger.error(f"B 站长连鉴权失败 code={code} payload={payload}")
        self._auth_event.set()

    def _handle_business(self, body: bytes) -> None:
        """``op=5``：反序列化 body，丢给上层回调。

        回调用 task_manager 派发，避免单条事件处理时间过长把 recv loop 卡住。
        """

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            logger.error(f"业务包 JSON 解析失败: {exc}; body={body[:200]!r}")
            return

        if not isinstance(payload, dict):
            logger.warning(f"业务包顶层不是 dict: {type(payload).__name__}")
            return

        tm = get_task_manager()
        tm.create_task(
            self._safe_invoke_event(payload),
            name=f"bilibili_live_adapter.event.{payload.get('cmd', 'UNKNOWN')}",
            daemon=True,
        )

    async def _safe_invoke_event(self, payload: dict[str, Any]) -> None:
        """包一层 try/except，避免 dispatcher 错误把整个 recv 拖崩。"""

        try:
            await self._on_event(payload)
        except Exception as exc:
            logger.error(
                f"on_event 回调异常 cmd={payload.get('cmd')}: {exc}",
                exc_info=True,
            )

    # ── 内部工具 ──────────────────────────────────────

    async def _safe_close_ws(self) -> None:
        """关 ws，吞掉所有异常。"""

        if self._ws is None:
            return
        try:
            await self._ws.close()
        except Exception as exc:
            logger.debug(f"关闭 ws 时异常（忽略）: {exc}")
        finally:
            self._ws = None


__all__ = [
    "BilibiliClient",
    "BilibiliClientError",
    "EventCallback",
]
