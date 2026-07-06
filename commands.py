"""bilibili_live_adapter 的运维控制命令。

提供 ``/bili reconnect`` 子命令，让最高权限用户在**不重启 bot** 的前提下
强制 B 站适配器断开当前长连并重新建立会话。

典型场景：

- 长连进入异常静默态（心跳还在但收不到弹幕），想手动触发一次干净的重连。
- 需要重新 ``/v2/app/start`` 拿新 game_id 重建会话。

权限：仅 ``OWNER`` 可用。
"""

from __future__ import annotations

from typing import cast

from src.app.plugin_system.api import adapter_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api.send_api import send_text
from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel

from .plugin import BilibiliLiveAdapter


logger = get_logger("bilibili_live_adapter.commands")


# B 站适配器组件签名：plugin_name:adapter:adapter_name。
_ADAPTER_SIGNATURE = "bilibili_live_adapter:adapter:bilibili_live_adapter"


class BilibiliCommand(BaseCommand):
    """``/B站`` 命令组：B 站直播适配器运维控制（仅 OWNER）。

    命令名用中文 ``B站`` 便于记忆；子命令 ``重连`` 同时保留英文别名
    ``reconnect``。即 ``/B站 重连`` 与 ``/B站 reconnect`` 等价。
    """

    command_name: str = "B站"
    command_description: str = (
        "B 站直播适配器运维控制：重连=强制断开并重连长连"
        "（用于长连静默或需要重建会话时手动接上，无需重启 bot）。"
    )
    permission_level: PermissionLevel = PermissionLevel.OWNER

    async def _reply(self, text: str) -> None:
        """向当前聊天流发送一条命令回执文本。"""

        await send_text(text, stream_id=self.stream_id)

    @cmd_route("重连")
    async def handle_reconnect_zh(self) -> tuple[bool, str]:
        """强制 B 站适配器断开当前长连并重新建立会话（中文别名）。"""
        return await self.handle_reconnect()

    @cmd_route("reconnect")
    async def handle_reconnect(self) -> tuple[bool, str]:
        """强制 B 站适配器断开当前长连并重新建立会话。"""

        adapter = adapter_api.get_adapter(_ADAPTER_SIGNATURE)
        if adapter is None:
            await self._reply(
                "未找到正在运行的 B 站适配器，请检查 bilibili_live_adapter 插件是否启用。"
            )
            return False, "adapter not active"

        bili_adapter = cast(BilibiliLiveAdapter, adapter)
        try:
            ok = await bili_adapter.force_reconnect()
        except Exception as exc:
            logger.error(f"强制重连 B 站适配器失败: {exc}", exc_info=True)
            await self._reply(f"强制重连失败：{exc}")
            return False, str(exc)

        if not ok:
            await self._reply(
                "B 站适配器当前处于禁用状态（[plugin].enabled=false），无法重连。"
            )
            return False, "adapter disabled"

        await self._reply(
            "✓ 已触发 B 站适配器重连：正在重新启动应用并建立长连，"
            "稍后应能恢复弹幕接收。"
        )
        logger.info("用户手动触发 B 站适配器重连")
        return True, "reconnect triggered"


__all__ = ["BilibiliCommand"]
