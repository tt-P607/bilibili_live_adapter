"""B 站直播弹幕 Adapter — 把 api / client / dispatcher 粘起来的总入口。

只入站不出站：B 站直播开放平台**不允许**第三方 bot 发弹幕，所以
``_send_platform_message`` 是 no-op。回应通过 anima_chatter 走 TTS + VTube
Studio。

为什么不用 ``WebSocketAdapterOptions`` 让基类自动管 ws：B 站协议是自定义
二进制（4 字节 pktLen + 16 字节包头 + body），还需要双心跳和 zlib/brotli
解压再切包，自动传输层不知道这些事。所以本类**自己开 ws、自己跑心跳、自
己解码包**，完成后再调 ``self.core_sink.send(envelope)`` 把消息送进核心。

⚠️ 重写 ``health_check()`` 是必需的：基类默认 ``is_connected()`` 在没有
``transport_config`` 时永远返回 ``False``，会触发健康检查无限重连。
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from mofox_wire import CoreSink, MessageEnvelope

from src.app.plugin_system.api.log_api import get_logger
from src.core.components.base import BaseAdapter, BasePlugin
from src.core.components.loader import register_plugin
from src.kernel.concurrency import get_task_manager

from .config import BilibiliLiveAdapterConfig
from .src.api import BilibiliApi, BilibiliApiError, StartResponse
from .src.client import BilibiliClient, BilibiliClientError
from .src.dispatcher import PLATFORM, BilibiliDispatcher


logger = get_logger("bilibili_live_adapter")


class BilibiliLiveAdapter(BaseAdapter):
    """B 站直播弹幕入站适配器。"""

    adapter_name = "bilibili_live_adapter"
    adapter_version = "0.1.0"
    adapter_author = "MoFox Team"
    adapter_description = (
        "B 站直播开放平台弹幕入站适配器（只入不出，回应交给 anima_chatter）"
    )
    platform = PLATFORM

    run_in_subprocess = False

    def __init__(
        self,
        core_sink: CoreSink,
        plugin: "BilibiliLiveAdapterPlugin | None" = None,
        **kwargs: Any,
    ) -> None:
        """不传 transport：自管 ws 长连。"""

        # 不传 transport 给 mofox-wire 基类（None 默认值），表示我们不要
        # 它自动 ws 管理逻辑。`_send_platform_message` 也会因此走我们自己
        # 的 no-op 实现。
        super().__init__(core_sink, plugin=plugin, **kwargs)

        # 运行时资源（on_adapter_loaded / start 时构造）
        self._api: BilibiliApi | None = None
        self._client: BilibiliClient | None = None
        self._dispatcher: BilibiliDispatcher | None = None
        self._start_resp: StartResponse | None = None

        # 自管的会话循环任务（负责 start_app → client.start → 等断 → 重连）
        self._session_task_info: Any | None = None
        self._stopping: bool = False

        # 重连指数退避状态
        self._consecutive_failures: int = 0

    # ── 配置读取 ──────────────────────────────────────

    def _get_config(self) -> BilibiliLiveAdapterConfig:
        """拿到本插件的配置；缺失时直接抛错（不允许带空凭证启动）。"""

        if self.plugin is None or self.plugin.config is None:
            raise RuntimeError("BilibiliLiveAdapter 启动失败：插件配置缺失")
        config = cast(BilibiliLiveAdapterConfig, self.plugin.config)
        bili = config.bilibili
        if not bili.access_key_id or not bili.access_key_secret:
            raise RuntimeError(
                "B 站凭证未填写：access_key_id / access_key_secret 都不能为空"
            )
        if not bili.id_code:
            raise RuntimeError("B 站凭证未填写：id_code（主播身份码）不能为空")
        if bili.app_id <= 0:
            raise RuntimeError(f"B 站凭证未填写：app_id 必须为正整数（当前 {bili.app_id}）")
        return config

    # ── 生命周期 ──────────────────────────────────────

    async def on_adapter_loaded(self) -> None:
        """构造 HTTP API 客户端 + dispatcher，但不在这里建立长连。

        长连放到 ``start()`` 之后的会话循环里，由 ``BaseAdapter.start()``
        触发。
        """

        config = self._get_config()
        bili = config.bilibili
        conn = config.connection

        self._api = BilibiliApi(
            host=bili.host,
            access_key_id=bili.access_key_id,
            access_key_secret=bili.access_key_secret,
            app_id=bili.app_id,
            id_code=bili.id_code,
            timeout=float(conn.request_timeout),
        )
        self._dispatcher = BilibiliDispatcher()
        logger.info("B 站 Adapter 配置就绪，等待 start() 建立长连")

    async def on_adapter_unloaded(self) -> None:
        """关闭 client / api，结束 game_id。"""

        await self._stop_session(end_app=True)

        if self._api is not None:
            try:
                await self._api.aclose()
            except Exception as exc:
                logger.warning(f"关闭 HTTP 客户端异常: {exc}")
            self._api = None

        self._dispatcher = None
        logger.info("B 站 Adapter 已卸载")

    async def start(self) -> None:
        """启动 BaseAdapter 公共流程 + 自管会话循环。"""

        # 注意：BaseAdapter.start() 自己会调 on_adapter_loaded()。所以这里
        # 调用 super().start() 就够了。不要在这里再调一次 on_adapter_loaded()。
        await super().start()

        self._stopping = False
        self._consecutive_failures = 0

        tm = get_task_manager()
        self._session_task_info = tm.create_task(
            self._session_loop(),
            name="bilibili_live_adapter.session",
            daemon=True,
        )

    async def stop(self) -> None:
        """先停自家会话循环，再走 BaseAdapter 公共停止。"""

        self._stopping = True
        await self._stop_session(end_app=True)
        await super().stop()

    # ── 健康检查（重写：不要看 self._ws） ─────────────

    async def health_check(self) -> bool:
        """返回当前会话是否健康。

        ``BaseAdapter.health_check`` 默认调 ``is_connected()``，那个是 mofox-wire
        在 ``_ws`` 上做判断的。我们自管 ws，不放在 ``self._ws`` 上，所以必
        须重写——否则基类的 30s 健康巡检会以为我们一直没连上，然后疯狂调
        ``reconnect()`` 把刚建好的 client 反复杀死。

        判定依据：
        - 我们正在停止 → True（避免 stop 期间被 reconnect 干扰）。
        - 会话循环还活着 + client 已鉴权 + ws 没断 → True。
        - 其它情况 → False（会话循环本身有重连逻辑，但允许基类也介入兜底）。
        """

        if self._stopping:
            return True
        if self._client is None:
            return False
        return bool(self._client.auth_ok and self._client.is_connected)

    async def reconnect(self) -> None:
        """基类健康检查不通过时会调这个。

        我们的会话循环本身就带断线重连，这里直接 no-op，避免双重重连互踩。
        """

        logger.debug("基类 reconnect 被触发，但本 adapter 自管重连，已忽略")

    # ── 出站：no-op ───────────────────────────────────

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """B 站不允许第三方 bot 发弹幕，整体丢弃出站。"""

        # 仅在 debug 级日志，避免每条核心回复都刷一行 warn。
        seg = envelope.get("message_segment")
        snippet: str
        if isinstance(seg, dict) and seg.get("type") == "text":
            snippet = str(seg.get("data") or "")[:30]
        else:
            snippet = ""
        logger.debug(f"忽略 B 站出站消息（平台不允许 bot 发弹幕）: {snippet}")

    # ── 入站：被基类 on_platform_message 调用 ────────

    async def from_platform_message(self, raw: Any) -> MessageEnvelope | None:  # type: ignore[override]
        """把 ``op=5`` 业务包翻译成 envelope。

        本 adapter 是自管 ws：``raw`` 实际上是 :class:`BilibiliClient` 直接
        传过来的 ``payload`` dict（``{"cmd": "...", "data": {...}}``）。
        """

        if self._dispatcher is None:
            return None
        if not isinstance(raw, dict):
            logger.warning(f"from_platform_message 收到非 dict: {type(raw).__name__}")
            return None
        return await self._dispatcher.dispatch(raw)

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """返回 Bot 在该平台上的身份信息。

        这里"Bot"其实是主播——我们是以主播视角作为 bot 在这个直播间出现的。
        """

        anchor_uname = ""
        anchor_room_id = 0
        if self._start_resp is not None:
            anchor_uname = self._start_resp.anchor_uname
            anchor_room_id = self._start_resp.anchor_room_id
        return {
            "bot_id": str(anchor_room_id) or "0",
            "bot_name": anchor_uname or "B 站主播",
            "platform": self.platform,
        }

    # ── 会话循环：start_app → client.start → 等断 → 退避重连 ──

    async def _session_loop(self) -> None:
        """长跑任务：维持一次"开应用 → 长连 → 心跳"会话；断了就退避重连。"""

        while not self._stopping:
            try:
                await self._run_one_session()
                # 一次会话主动结束（被 stop 调用）：直接退出。
                if self._stopping:
                    break
                # 否则视为意外断开，进入退避重连分支。
            except asyncio.CancelledError:
                break
            except (BilibiliApiError, BilibiliClientError) as exc:
                logger.warning(f"B 站会话异常: {exc}")
            except Exception as exc:
                logger.error(f"B 站会话未预期异常: {exc}", exc_info=True)

            if self._stopping:
                break

            if not self._auto_reconnect_enabled():
                logger.info("auto_reconnect 已关闭，退出会话循环")
                break

            await self._sleep_with_backoff()

        logger.info("B 站会话循环退出")

    async def _run_one_session(self) -> None:
        """完整跑一次会话：调 start_app + 建 client + start + 等到 client 退出。"""

        if self._api is None or self._dispatcher is None:
            raise RuntimeError("API / dispatcher 尚未初始化（on_adapter_loaded 没跑过？）")

        config = self._get_config()
        conn = config.connection

        # 1) 调 /v2/app/start
        logger.info("调用 /v2/app/start 启动应用")
        start_resp = await self._api.start_app()
        self._start_resp = start_resp
        self._dispatcher.update_room_context(
            room_id=start_resp.anchor_room_id,
            anchor_uname=start_resp.anchor_uname,
        )
        logger.info(
            f"start_app 成功 game_id={start_resp.game_id} "
            f"主播={start_resp.anchor_uname} room={start_resp.anchor_room_id}"
        )

        # 2) 建 client，回调里把业务 payload 喂给 BaseAdapter.on_platform_message。
        #    走基类的 on_platform_message → 自动 from_platform_message → core_sink.send。
        self._client = BilibiliClient(
            api=self._api,
            on_event=self.on_platform_message,
            heartbeat_ws_interval=float(conn.heartbeat_ws_interval),
            heartbeat_app_interval=float(conn.heartbeat_app_interval),
        )

        try:
            # 3) 建立长连 + 鉴权 + 心跳 + recv 循环
            await self._client.start(start_resp)
            self._consecutive_failures = 0

            # 4) 等 client 自己结束（recv 退出 / 主动 stop）
            await self._client.wait_closed()
        finally:
            # 不管什么原因结束，先尝试结束 game_id 释放资源。
            try:
                await self._client.stop()
            except Exception as exc:
                logger.debug(f"停止 client 异常: {exc}")
            # 注意：B 站文档建议每次会话结束都调一次 /end 释放占用。
            # 即使后面要重连，也是用新的 game_id；旧的不再有用。
            await self._safe_end_app(start_resp.game_id)
            self._client = None
            self._start_resp = None

    async def _stop_session(self, *, end_app: bool) -> None:
        """关闭当前会话与会话循环。

        Args:
            end_app: 是否调用 ``/v2/app/end`` 释放 game_id（卸载时为 True；
                仅断重连之间无需调，由 ``_run_one_session`` 的 finally 自管）。
        """

        if self._client is not None:
            try:
                await self._client.stop()
            except Exception as exc:
                logger.debug(f"关闭 client 异常: {exc}")

        if self._session_task_info is not None:
            tm = get_task_manager()
            try:
                tm.cancel_task(self._session_task_info.task_id)
            except Exception:
                pass
            self._session_task_info = None

        if end_app and self._start_resp is not None and self._api is not None:
            await self._safe_end_app(self._start_resp.game_id)

        self._client = None
        self._start_resp = None

    async def _safe_end_app(self, game_id: str) -> None:
        """调 ``/v2/app/end``；任何异常只记日志。"""

        if not game_id or self._api is None:
            return
        try:
            await self._api.end_app(game_id)
            logger.info(f"已结束 game_id={game_id}")
        except Exception as exc:
            logger.warning(f"end_app 失败 game_id={game_id}: {exc}")

    def _auto_reconnect_enabled(self) -> bool:
        """配置开关：是否允许长连断开后自动重连。"""

        try:
            config = self._get_config()
        except Exception:
            return False
        return bool(config.connection.auto_reconnect)

    async def _sleep_with_backoff(self) -> None:
        """指数退避：连续失败次数越多，等待越久（封顶 ``reconnect_max_delay``）。"""

        config = self._get_config()
        conn = config.connection
        self._consecutive_failures += 1
        # 1, 2, 4, 8, ... × initial_delay；超过封顶截断。
        delay = float(conn.reconnect_initial_delay) * (2 ** (self._consecutive_failures - 1))
        delay = min(delay, float(conn.reconnect_max_delay))
        logger.info(
            f"等待 {delay:.1f}s 后重连（连续失败 {self._consecutive_failures} 次）"
        )
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise


@register_plugin
class BilibiliLiveAdapterPlugin(BasePlugin):
    """B 站直播弹幕适配器插件。"""

    plugin_name = "bilibili_live_adapter"
    plugin_version = "0.1.0"
    plugin_author = "MoFox Team"
    plugin_description = "B 站直播开放平台弹幕入站适配器（基于 Neo-MoFox）"
    configs = [BilibiliLiveAdapterConfig]

    def get_components(self) -> list[type]:
        """返回插件内所有组件类。"""

        return [BilibiliLiveAdapter]


__all__ = [
    "BilibiliLiveAdapter",
    "BilibiliLiveAdapterPlugin",
]
