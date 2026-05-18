"""把 ``op=5`` 业务包的 ``cmd`` 路由成统一的 ``MessageEnvelope``。

第一版只处理 ``LIVE_OPEN_PLATFORM_DM``（弹幕）。礼物 / SC / 上舰留占位，
后续按 :mod:`docs.DEVELOPMENT` "扩展事件" 章节扩展。

设计要点：

- **平台标识 `platform = "bilibili_live"`**：要和 :class:`plugin.BilibiliLiveAdapter`
  的 ``platform`` 类属性一致；下游 chatter 用它做平台分流。
- **群聊视角**：把整个直播间当作"一个群"，``group_id = room_id``，群名 = 主播昵称。
  这样下游 anima_chatter / dfc 那种习惯于群聊语义的 chatter 就能直接复用。
- **用户标识用 ``open_id``**：B 站给的脱敏 ID，跨直播间稳定，比 ``uid`` 更适合长期记忆。
  uid 只塞 ``additional_config`` 备查。
- **不入库的事件返回 None**：``on_platform_message`` 看到 None 会自动跳过 ``core_sink.send``。
  写新 cmd 时遇到不想丢给核心的（比如点赞高频流）直接 return None 即可。
"""

from __future__ import annotations

from typing import Any

from mofox_wire import MessageBuilder, MessageEnvelope
from mofox_wire.types import UserRole

from src.kernel.logger import get_logger


logger = get_logger("bilibili_live_adapter.dispatcher")


# 与 :class:`plugin.BilibiliLiveAdapter.platform` 必须一致。
PLATFORM = "bilibili_live"


# ── 已知 cmd ─────────────────────────────────────────
CMD_DM = "LIVE_OPEN_PLATFORM_DM"
CMD_GIFT = "LIVE_OPEN_PLATFORM_SEND_GIFT"
CMD_SUPER_CHAT = "LIVE_OPEN_PLATFORM_SUPER_CHAT"
CMD_GUARD = "LIVE_OPEN_PLATFORM_GUARD"
CMD_LIKE = "LIVE_OPEN_PLATFORM_LIKE"


class BilibiliDispatcher:
    """把 B 站业务包翻译成 ``MessageEnvelope``。

    构造时拿到一些上下文（房间号 / 主播昵称等），dispatch 时根据 cmd 路由。
    """

    def __init__(self, *, room_id: int = 0, anchor_uname: str = "") -> None:
        """记录 start_app 拿到的房间上下文。

        Args:
            room_id: 主播的真实直播间号；当作群 ID 用。
            anchor_uname: 主播昵称；当作群名用，便于日志和提示词渲染。
        """

        self._room_id = int(room_id or 0)
        self._anchor_uname = str(anchor_uname or "")

    def update_room_context(self, *, room_id: int, anchor_uname: str) -> None:
        """重连后拿到新的 ``room_id`` / ``anchor_uname`` 时调用。

        理论上同一个主播 room_id 不变；但鉴权重新走 start 流程后，谨慎起见
        让外层有机会刷新。
        """

        self._room_id = int(room_id or 0)
        self._anchor_uname = str(anchor_uname or "")

    async def dispatch(self, payload: dict[str, Any]) -> MessageEnvelope | None:
        """把 ``op=5`` body 路由成一条 ``MessageEnvelope``。

        Args:
            payload: ``{"cmd": "...", "data": {...}}``。

        Returns:
            转好的 ``MessageEnvelope``；不需要丢给核心的事件返回 ``None``。
        """

        cmd = str(payload.get("cmd", ""))
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            logger.warning(f"业务包 data 不是 dict: cmd={cmd} data_type={type(data).__name__}")
            return None

        if cmd == CMD_DM:
            return self._build_dm_envelope(data)
        if cmd == CMD_LIKE:
            # 高频流；第一版直接丢弃。
            return None
        if cmd in (CMD_GIFT, CMD_SUPER_CHAT, CMD_GUARD):
            # 占位：等下游消费方就绪后再补；此时也 swallow，避免噪音。
            logger.debug(f"暂不处理的事件 cmd={cmd}")
            return None

        logger.debug(f"未知 cmd 已忽略: {cmd}")
        return None

    # ── DM 弹幕 ──────────────────────────────────────

    def _build_dm_envelope(self, data: dict[str, Any]) -> MessageEnvelope | None:
        """``LIVE_OPEN_PLATFORM_DM`` → 一条群消息形态的 envelope。

        关键字段映射（详见 :mod:`docs.API` § 5.1）：

        - ``open_id`` → ``user_info.user_id``：脱敏 ID，跨直播间稳定。
        - ``uid`` → ``additional_config["bilibili_uid"]``：原始 uid 备查。
        - ``msg_id`` → ``message_info.message_id``：可用于幂等检查。
        - ``msg`` → ``SegPayload(type="text")``：弹幕正文。
        - ``room_id`` → ``group_info.group_id``：直播间号当作群 ID。
        """

        msg_text = str(data.get("msg") or "").strip()
        if not msg_text:
            logger.debug("收到空弹幕，跳过")
            return None

        open_id = str(data.get("open_id") or "")
        uname = str(data.get("uname") or "")
        uid = data.get("uid")
        uface = str(data.get("uface") or "")
        msg_id = str(data.get("msg_id") or "")
        timestamp = data.get("timestamp")
        room_id = int(data.get("room_id") or self._room_id or 0)
        guard_level = int(data.get("guard_level") or 0)
        fans_medal_level = int(data.get("fans_medal_level") or 0)
        fans_medal_name = str(data.get("fans_medal_name") or "")
        dm_type = int(data.get("dm_type") or 0)
        emoji_img_url = str(data.get("emoji_img_url") or "")

        # 没有 open_id 时退化用 uid（极少见，但要保证 user_id 非空，否则下游可能拒收）。
        user_id = open_id or (str(uid) if uid is not None else "anon")

        # 舰长用 OPERATOR 角色，剩下的统一 MEMBER。这样 dfc / anima_chatter 想区分
        # 舰长身份时可以直接看 user_role。
        role = UserRole.OPERATOR if guard_level > 0 else UserRole.MEMBER

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )

        if msg_id:
            builder.message_id(msg_id)
        if isinstance(timestamp, (int, float)):
            # B 站给的是秒，先转毫秒；同时也写到 message_info.time（builder 默认会写当前时间）。
            builder.timestamp_ms(int(float(timestamp) * 1000))

        builder.from_user(
            user_id=user_id,
            platform=PLATFORM,
            nickname=uname,
            user_avatar=uface,
            role=role,
        )

        if room_id:
            builder.from_group(
                group_id=str(room_id),
                platform=PLATFORM,
                name=self._anchor_uname or f"B 站直播间 {room_id}",
            )

        envelope = builder.build()

        # 平台专属字段（粉丝勋章 / 舰长等级 / 表情包弹幕）塞到 additional_config，
        # 后续 chatter 想用直接在 message_info.additional_config 里取。
        additional: dict[str, Any] = {
            "bilibili_uid": uid,
            "guard_level": guard_level,
            "fans_medal_level": fans_medal_level,
            "fans_medal_name": fans_medal_name,
            "dm_type": dm_type,
            "emoji_img_url": emoji_img_url,
        }
        envelope["message_info"]["additional_config"] = additional
        envelope["raw_message"] = data

        logger.info(
            f"收到弹幕 [{self._anchor_uname or room_id}] "
            f"{uname}({user_id[:8]}...) guard={guard_level} 说: {msg_text}"
        )
        return envelope


__all__ = [
    "BilibiliDispatcher",
    "CMD_DM",
    "CMD_GIFT",
    "CMD_GUARD",
    "CMD_LIKE",
    "CMD_SUPER_CHAT",
    "PLATFORM",
]
