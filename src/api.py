"""B 站开放平台 HTTP API 客户端：签名 + start / heartbeat / end。

详见 :mod:`docs.API` § 2 (鉴权) 与 § 3 (三个接口)。

签名规则关键点（容易踩的坑）：

1. ``x-bili-content-md5`` 算的是**请求体字符串**的 md5（小写 hex），
   不是 dict 的 md5。所以一定要先 ``json.dumps(body)`` 再 md5。
2. 待签名字符串里 6 个 ``x-bili-`` 头按**字典序**排，每行 ``key:value``，
   行间 ``\\n`` 连接，**首尾都不加换行**。
3. 用 ``access_key_secret`` 做 HMAC-SHA256，结果转**小写** hex 作为
   ``Authorization`` 头。
4. ``x-bili-timestamp`` 是秒级 UNIX 时间戳，与服务端误差 ≤10 分钟。
5. ``x-bili-signature-nonce`` 必须**全局唯一**，建议 UUID4。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import httpx

from src.kernel.logger import get_logger


logger = get_logger("bilibili_live_adapter.api")


_API_START = "/v2/app/start"
_API_HEARTBEAT = "/v2/app/heartbeat"
_API_END = "/v2/app/end"


@dataclass
class StartResponse:
    """``/v2/app/start`` 解析后的关键产出。

    详见 :mod:`docs.API` § 3.1 的返回结构。
    """

    game_id: str
    """长连/心跳生命周期的标识；后续 heartbeat / end 都要带它。"""

    auth_body: str
    """鉴权信息，作为 op=7 包的 body 原样发送。"""

    wss_links: list[str]
    """长连地址列表，第一版取 ``[0]``。容灾时可遍历。"""

    anchor_room_id: int
    """主播的真实房间号，记日志用，不影响协议。"""

    anchor_uname: str
    """主播昵称，同样仅用于日志。"""


class BilibiliApiError(RuntimeError):
    """B 站 API 业务错误（``code != 0``）。

    携带 code / message / request_id 便于排错；具体含义看 docs/API.md § 6。
    """

    def __init__(self, code: int, message: str, request_id: str = "") -> None:
        super().__init__(f"B 站 API 错误 code={code} message={message} request_id={request_id}")
        self.code = code
        self.message = message
        self.request_id = request_id


class BilibiliApi:
    """B 站开放平台 HTTP 客户端。

    实例化后通过 ``async with`` 或显式 ``await aclose()`` 释放底层 httpx。
    """

    def __init__(
        self,
        *,
        host: str,
        access_key_id: str,
        access_key_secret: str,
        app_id: int,
        id_code: str,
        timeout: float = 15.0,
    ) -> None:
        """记录凭证；不在构造时建立连接（连接由 httpx 自动管理）。

        Args:
            host: API 主机（含 https://），如 https://live-open.biliapi.com
            access_key_id: AccessKey ID
            access_key_secret: AccessKey Secret，签名密钥
            app_id: 应用 ID（整数）
            id_code: 主播身份码
            timeout: 每个 HTTP 请求超时（秒）
        """

        if not access_key_id or not access_key_secret:
            raise ValueError("access_key_id / access_key_secret 不能为空")
        if not id_code:
            raise ValueError("id_code 不能为空")
        if app_id <= 0:
            raise ValueError(f"app_id 必须为正整数，传入 {app_id}")

        self._host = host.rstrip("/")
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._app_id = app_id
        self._id_code = id_code
        # httpx 客户端按实例持有；连接复用更稳。
        self._client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        """关闭底层 httpx 客户端。"""

        await self._client.aclose()

    # ── 三个核心接口 ──────────────────────────────────

    async def start_app(self) -> StartResponse:
        """``POST /v2/app/start`` — 开启应用，拿 game_id + 长连信息。"""

        body = {"code": self._id_code, "app_id": self._app_id}
        data = await self._post(_API_START, body)

        try:
            game_info = data["game_info"]
            ws_info = data["websocket_info"]
            anchor_info = data.get("anchor_info") or {}
            return StartResponse(
                game_id=str(game_info["game_id"]),
                auth_body=str(ws_info["auth_body"]),
                wss_links=[str(link) for link in ws_info["wss_link"]],
                anchor_room_id=int(anchor_info.get("room_id", 0) or 0),
                anchor_uname=str(anchor_info.get("uname", "") or ""),
            )
        except (KeyError, TypeError) as exc:
            raise RuntimeError(f"start_app 返回结构异常: {data!r}") from exc

    async def app_heartbeat(self, game_id: str) -> None:
        """``POST /v2/app/heartbeat`` — 维持 game_id 活跃。每 20s 调一次。"""

        await self._post(_API_HEARTBEAT, {"game_id": game_id})

    async def end_app(self, game_id: str) -> None:
        """``POST /v2/app/end`` — 关闭应用，释放 game_id 占用。"""

        await self._post(_API_END, {"game_id": game_id, "app_id": self._app_id})

    # ── 内部 ──────────────────────────────────────────

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """统一 POST：序列化 body → 签名头 → 发送 → 校验 code。

        失败时抛 :class:`BilibiliApiError`。
        """

        # 必须用 json.dumps 拿到字符串再算 md5；用 dict 直接 md5 会得到错误
        # 的字节流（这是 4012 错误的常见原因）。
        body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        headers = self._sign(body_str)

        url = f"{self._host}{path}"
        logger.debug(f"POST {url} body={body_str}")

        resp = await self._client.post(url, content=body_str.encode("utf-8"), headers=headers)
        resp.raise_for_status()
        result = resp.json()

        code = int(result.get("code", -1))
        if code != 0:
            raise BilibiliApiError(
                code=code,
                message=str(result.get("message", "")),
                request_id=str(result.get("request_id", "")),
            )

        return result.get("data", {}) or {}

    def _sign(self, body_str: str) -> dict[str, str]:
        """根据请求体字符串计算签名头。

        计算流程见模块 docstring。
        """

        # 1) 算 body md5（小写 hex）
        md5_hex = hashlib.md5(body_str.encode("utf-8")).hexdigest()

        # 2) 6 个 x-bili-* 头按字典序
        ts = str(int(time.time()))
        nonce = str(uuid.uuid4())
        meta_headers: dict[str, str] = {
            "x-bili-accesskeyid": self._access_key_id,
            "x-bili-content-md5": md5_hex,
            "x-bili-signature-method": "HMAC-SHA256",
            "x-bili-signature-nonce": nonce,
            "x-bili-signature-version": "1.0",
            "x-bili-timestamp": ts,
        }

        # 3) 拼成待签名字符串：行间 \n，首尾不加 \n
        sign_lines = [f"{k}:{meta_headers[k]}" for k in sorted(meta_headers)]
        sign_str = "\n".join(sign_lines)

        # 4) HMAC-SHA256（小写 hex）
        signature = hmac.new(
            self._access_key_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            digestmod=sha256,
        ).hexdigest()

        # 5) 组装最终头
        return {
            **meta_headers,
            "Authorization": signature,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }


__all__ = [
    "BilibiliApi",
    "BilibiliApiError",
    "StartResponse",
]
