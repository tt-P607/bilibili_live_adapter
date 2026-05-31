"""把 ``op=5`` 业务包的 ``cmd`` 路由成统一的 ``MessageEnvelope``。

支持的事件：

- ``LIVE_OPEN_PLATFORM_DM`` 弹幕 → 文本消息
- ``LIVE_OPEN_PLATFORM_SEND_GIFT`` 礼物 → 描述性文本消息（``[送出礼物] xxx ×N``）
- ``LIVE_OPEN_PLATFORM_SUPER_CHAT`` 醒目留言（SC）→ 描述性文本消息（``[SC ¥30/2分钟] ...``）
- ``LIVE_OPEN_PLATFORM_GUARD`` 上舰 → 描述性文本消息（``[开通舰长] 舰长 ×1月``）
- ``LIVE_OPEN_PLATFORM_LIKE`` 点赞 → 不下发 envelope；累加内部计数器供
  :class:`plugin.BilibiliLiveAdapter` 通过 ``system_reminder`` 注入到 prompt。

设计要点：

- **平台标识 ``platform = "bilibili_live"``**：要和 :class:`plugin.BilibiliLiveAdapter`
  的 ``platform`` 类属性一致；下游 chatter 用它做平台分流。
- **群聊视角**：把整个直播间当作"一个群"，``group_id = room_id``，群名固定为
  ``"B 站直播间 {room_id}"``（不再使用主播昵称，因为开放平台返回的是主播账号
  名，并不是真正的"直播间标题"）。
- **用户标识用 ``open_id``**：B 站给的脱敏 ID，跨直播间稳定，比 ``uid`` 更适合长期记忆。
  uid 只塞 ``additional_config`` 备查。
- **不入库的事件返回 None**：``on_platform_message`` 看到 None 会自动跳过 ``core_sink.send``。
- **礼物 / SC 也走"群消息"形态**：复用 DM 那套 ``MessageBuilder``，``message_segment``
  顶层是 ``text``，但文本前缀加 ``[送出礼物]`` / ``[SC ¥xxx]`` 等标签，让模型一眼能区分。
  原始字段（``gift_id``、``price``、``rmb`` 等）一律塞 ``additional_config``，方便后续
  扩展专门的事件回应逻辑。
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


# ── 上舰等级名映射（B 站官方称呼） ─────────────────
# guard_level: 0=非舰长 / 1=总督 / 2=提督 / 3=舰长
_GUARD_LEVEL_NAME: dict[int, str] = {
    1: "总督",
    2: "提督",
    3: "舰长",
}


def _format_price_yuan(price_gold: int | float) -> str:
    """把"金瓜子"金额格式化为"¥X.X 元"字符串。

    1 元人民币 = 1000 金瓜子（B 站开放平台口径）。返回保留 1 位小数；
    若结果为整数（如 28.0）也保留 1 位小数 ``28.0``，避免和秒数等混淆。
    """

    if not price_gold:
        return "¥0.0"
    yuan = float(price_gold) / 1000.0
    return f"¥{yuan:.1f}"


def _format_sc_duration(start_ts: int | float | None, end_ts: int | float | None) -> str:
    """根据 SC 的 ``start_time`` / ``end_time`` 渲染人类可读的总时长。

    返回格式：``"X分钟"`` 或 ``"Xs"``；时长不可推断时返回空串。
    """

    try:
        start = int(start_ts) if start_ts is not None else 0
        end = int(end_ts) if end_ts is not None else 0
    except (TypeError, ValueError):
        return ""
    duration = end - start
    if duration <= 0:
        return ""
    if duration % 60 == 0:
        return f"{duration // 60}分钟"
    if duration >= 60:
        # 边缘情况：不是整分钟，按 "Xm Ys" 显示
        minutes, seconds = divmod(duration, 60)
        return f"{minutes}分{seconds}秒"
    return f"{duration}秒"


class BilibiliDispatcher:
    """把 B 站业务包翻译成 ``MessageEnvelope``。

    构造时拿到一些上下文（房间号 / 主播昵称等），dispatch 时根据 cmd 路由。
    点赞事件不会下发 envelope，而是在内部累加 ``total_likes`` 计数，由
    :class:`plugin.BilibiliLiveAdapter` 周期性把这个数注入 prompt 的
    ``system_reminder``。
    """

    def __init__(self, *, room_id: int = 0, anchor_uname: str = "") -> None:
        """记录 start_app 拿到的房间上下文。

        Args:
            room_id: 主播的真实直播间号；当作群 ID 用。
            anchor_uname: 主播昵称；保留备用（日志、bot_info 等），但不再用作群名。
        """

        self._room_id = int(room_id or 0)
        self._anchor_uname = str(anchor_uname or "")
        self._total_likes: int = 0

    def update_room_context(self, *, room_id: int, anchor_uname: str) -> None:
        """重连后拿到新的 ``room_id`` / ``anchor_uname`` 时调用。

        理论上同一个主播 room_id 不变；但鉴权重新走 start 流程后，谨慎起见
        让外层有机会刷新。新会话开始时同时清零累计点赞数（直播分场重新计数
        更符合直觉）。
        """

        self._room_id = int(room_id or 0)
        self._anchor_uname = str(anchor_uname or "")
        self._total_likes = 0

    @property
    def total_likes(self) -> int:
        """当前会话累计收到的点赞数（B 站 LIKE 事件累加值）。"""

        return self._total_likes

    @property
    def room_id(self) -> int:
        """当前直播间号；用于 reminder 文案中标注上下文。"""

        return self._room_id

    @property
    def anchor_uname(self) -> str:
        """当前主播昵称；用于 reminder 文案中标注上下文。"""

        return self._anchor_uname

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
        if cmd == CMD_GIFT:
            return self._build_gift_envelope(data)
        if cmd == CMD_SUPER_CHAT:
            return self._build_super_chat_envelope(data)
        if cmd == CMD_GUARD:
            return self._build_guard_envelope(data)
        if cmd == CMD_LIKE:
            self._handle_like(data)
            return None

        logger.debug(f"未知 cmd 已忽略: {cmd}")
        return None

    # ── 共享：构造 user / group / additional ─────────

    def _resolve_user_id(self, data: dict[str, Any]) -> str:
        """从事件 data 中拿到 user_id（优先 ``open_id``，缺失时降级 ``uid``）。"""

        open_id = str(data.get("open_id") or "")
        if open_id:
            return open_id
        uid = data.get("uid")
        if uid is not None:
            return str(uid)
        return "anon"

    def _apply_user(
        self,
        builder: MessageBuilder,
        *,
        user_id: str,
        uname: str,
        uface: str,
        guard_level: int,
        force_role: UserRole | None = None,
    ) -> None:
        """把发送者信息塞进 builder。

        ``force_role`` 用于强制指定角色（例如上舰事件本身的发起者必为
        ``OPERATOR``）；否则按 ``guard_level`` 自动判定。
        """

        if force_role is not None:
            role = force_role
        elif guard_level > 0:
            role = UserRole.OPERATOR
        else:
            role = UserRole.MEMBER

        builder.from_user(
            user_id=user_id,
            platform=PLATFORM,
            nickname=uname,
            user_avatar=uface,
            role=role,
        )

    def _apply_group(self, builder: MessageBuilder, *, room_id: int) -> None:
        """统一把直播间映射成"群"。

        群名固定使用 ``"B 站直播间 {room_id}"`` —— 开放平台返回的是主播账号
        名，并不是直播间标题，避免误导模型。
        """

        if not room_id:
            return
        builder.from_group(
            group_id=str(room_id),
            platform=PLATFORM,
            name=f"B 站直播间 {room_id}",
        )

    def _build_common_additional(self, data: dict[str, Any]) -> dict[str, Any]:
        """提取所有事件都该带的"平台共享字段"。

        各 ``_build_*_envelope`` 拿到这个 dict 后再补充自家专属字段。
        """

        return {
            "bilibili_uid": data.get("uid"),
            "guard_level": int(data.get("guard_level") or 0),
            "fans_medal_level": int(data.get("fans_medal_level") or 0),
            "fans_medal_name": str(data.get("fans_medal_name") or ""),
        }

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

        uname = str(data.get("uname") or "")
        uface = str(data.get("uface") or "")
        msg_id = str(data.get("msg_id") or "")
        timestamp = data.get("timestamp")
        room_id = int(data.get("room_id") or self._room_id or 0)
        guard_level = int(data.get("guard_level") or 0)
        dm_type = int(data.get("dm_type") or 0)
        emoji_img_url = str(data.get("emoji_img_url") or "")
        user_id = self._resolve_user_id(data)

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )

        if msg_id:
            builder.message_id(msg_id)
        if isinstance(timestamp, (int, float)):
            # B 站给的是秒，转毫秒。
            builder.timestamp_ms(int(float(timestamp) * 1000))

        self._apply_user(
            builder,
            user_id=user_id,
            uname=uname,
            uface=uface,
            guard_level=guard_level,
        )
        self._apply_group(builder, room_id=room_id)

        envelope = builder.build()

        additional = self._build_common_additional(data)
        additional.update(
            {
                "event_type": "danmaku",
                "dm_type": dm_type,
                "emoji_img_url": emoji_img_url,
            }
        )
        envelope["message_info"]["additional_config"] = additional
        envelope["raw_message"] = data

        logger.info(
            f"收到弹幕 [room={room_id}] "
            f"{uname}({user_id[:8]}...) guard={guard_level} 说: {msg_text}"
        )
        return envelope

    # ── 礼物 ────────────────────────────────────────

    def _build_gift_envelope(self, data: dict[str, Any]) -> MessageEnvelope | None:
        """``LIVE_OPEN_PLATFORM_SEND_GIFT`` → 描述性文本消息。

        文案样式：

        - 付费礼物：``[送出礼物] 礼花 ×1 (¥28.0)``
        - 免费礼物：``[送出礼物] 小心心 ×10 (免费)``
        """

        gift_name = str(data.get("gift_name") or "未知礼物")
        gift_num = int(data.get("gift_num") or 1)
        price_gold = data.get("price") or 0
        paid = bool(data.get("paid"))
        uname = str(data.get("uname") or "")
        uface = str(data.get("uface") or "")
        msg_id = str(data.get("msg_id") or "")
        timestamp = data.get("timestamp")
        room_id = int(data.get("room_id") or self._room_id or 0)
        guard_level = int(data.get("guard_level") or 0)
        user_id = self._resolve_user_id(data)

        if paid:
            tag = f"(¥{float(price_gold) / 1000.0:.1f})"
        else:
            tag = "(免费)"
        msg_text = f"[送出礼物] {gift_name} ×{gift_num} {tag}"

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )

        if msg_id:
            builder.message_id(msg_id)
        if isinstance(timestamp, (int, float)):
            builder.timestamp_ms(int(float(timestamp) * 1000))

        self._apply_user(
            builder,
            user_id=user_id,
            uname=uname,
            uface=uface,
            guard_level=guard_level,
        )
        self._apply_group(builder, room_id=room_id)

        envelope = builder.build()

        additional = self._build_common_additional(data)
        additional.update(
            {
                "event_type": "gift",
                "gift_id": data.get("gift_id"),
                "gift_name": gift_name,
                "gift_num": gift_num,
                "price_gold": int(price_gold or 0),
                "price_yuan": float(price_gold or 0) / 1000.0,
                "paid": paid,
                "combo_gift": bool(data.get("combo_gift")),
            }
        )
        envelope["message_info"]["additional_config"] = additional
        envelope["raw_message"] = data

        logger.info(
            f"收到礼物 [room={room_id}] "
            f"{uname}({user_id[:8]}...) → {gift_name} ×{gift_num} "
            f"{'paid' if paid else 'free'} ({_format_price_yuan(price_gold)})"
        )
        return envelope

    # ── SC（醒目留言） ──────────────────────────────

    def _build_super_chat_envelope(self, data: dict[str, Any]) -> MessageEnvelope | None:
        """``LIVE_OPEN_PLATFORM_SUPER_CHAT`` → 描述性文本消息。

        文案样式：``[SC ¥30/2分钟] 主播加油，今天的歌真好听``。
        SC 留言为空时只显示标签部分。
        """

        sc_message = str(data.get("message") or "").strip()
        rmb = data.get("rmb") or 0
        start_ts = data.get("start_time")
        end_ts = data.get("end_time")
        uname = str(data.get("uname") or "")
        uface = str(data.get("uface") or "")
        msg_id = str(data.get("msg_id") or "")
        room_id = int(data.get("room_id") or self._room_id or 0)
        guard_level = int(data.get("guard_level") or 0)
        user_id = self._resolve_user_id(data)

        duration_text = _format_sc_duration(start_ts, end_ts)
        try:
            rmb_int = int(rmb)
        except (TypeError, ValueError):
            rmb_int = 0
        if duration_text:
            tag = f"[SC ¥{rmb_int}/{duration_text}]"
        else:
            tag = f"[SC ¥{rmb_int}]"
        msg_text = f"{tag} {sc_message}".rstrip()

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )

        if msg_id:
            builder.message_id(msg_id)
        # SC 没有顶层 timestamp，用 start_time 作为消息时间。
        if isinstance(start_ts, (int, float)) and start_ts:
            builder.timestamp_ms(int(float(start_ts) * 1000))

        self._apply_user(
            builder,
            user_id=user_id,
            uname=uname,
            uface=uface,
            guard_level=guard_level,
        )
        self._apply_group(builder, room_id=room_id)

        envelope = builder.build()

        additional = self._build_common_additional(data)
        additional.update(
            {
                "event_type": "super_chat",
                "rmb": rmb_int,
                "start_time": int(start_ts) if isinstance(start_ts, (int, float)) else None,
                "end_time": int(end_ts) if isinstance(end_ts, (int, float)) else None,
                "duration_seconds": (
                    int(end_ts) - int(start_ts)
                    if isinstance(start_ts, (int, float))
                    and isinstance(end_ts, (int, float))
                    else None
                ),
                "sc_message": sc_message,
            }
        )
        envelope["message_info"]["additional_config"] = additional
        envelope["raw_message"] = data

        logger.info(
            f"收到 SC [room={room_id}] "
            f"{uname}({user_id[:8]}...) ¥{rmb_int}{'/' + duration_text if duration_text else ''}: "
            f"{sc_message}"
        )
        return envelope

    # ── 上舰 ────────────────────────────────────────

    def _build_guard_envelope(self, data: dict[str, Any]) -> MessageEnvelope | None:
        """``LIVE_OPEN_PLATFORM_GUARD`` → 描述性文本消息。

        ``user_info`` 是嵌套字段（与其他事件不同），需要单独取。文案样式：
        ``[开通舰长] 舰长 ×1月``。
        """

        user_info = data.get("user_info") or {}
        if not isinstance(user_info, dict):
            user_info = {}
        guard_level = int(data.get("guard_level") or 0)
        guard_num = int(data.get("guard_num") or 1)
        guard_unit = str(data.get("guard_unit") or "月")
        timestamp = data.get("timestamp")
        msg_id = str(data.get("msg_id") or "")
        room_id = self._room_id  # 上舰事件 data 顶层没有 room_id，沿用会话上下文

        guard_name = _GUARD_LEVEL_NAME.get(guard_level, "舰长")
        msg_text = f"[开通舰长] {guard_name} ×{guard_num}{guard_unit}"

        # user_info 字段名与其他事件保持一致（uname / uid / open_id / uface）
        uname = str(user_info.get("uname") or "")
        uface = str(user_info.get("uface") or "")
        synthetic_user_data = {
            "open_id": user_info.get("open_id"),
            "uid": user_info.get("uid"),
        }
        user_id = self._resolve_user_id(synthetic_user_data)

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )

        if msg_id:
            builder.message_id(msg_id)
        if isinstance(timestamp, (int, float)):
            builder.timestamp_ms(int(float(timestamp) * 1000))

        # 上舰事件本身的发起人天然就是舰长，强制 OPERATOR
        self._apply_user(
            builder,
            user_id=user_id,
            uname=uname,
            uface=uface,
            guard_level=guard_level,
            force_role=UserRole.OPERATOR,
        )
        self._apply_group(builder, room_id=room_id)

        envelope = builder.build()

        # 上舰事件的 fans_medal 是顶层字段（不是嵌在 user_info 里）。
        additional = {
            "bilibili_uid": user_info.get("uid"),
            "guard_level": guard_level,
            "fans_medal_level": int(data.get("fans_medal_level") or 0),
            "fans_medal_name": str(data.get("fans_medal_name") or ""),
            "event_type": "guard",
            "guard_name": guard_name,
            "guard_num": guard_num,
            "guard_unit": guard_unit,
        }
        envelope["message_info"]["additional_config"] = additional
        envelope["raw_message"] = data

        logger.info(
            f"收到上舰 [room={room_id}] "
            f"{uname}({user_id[:8]}...) → {guard_name} ×{guard_num}{guard_unit}"
        )
        return envelope

    # ── 点赞（不下发 envelope，只累加计数） ───────────

    def _handle_like(self, data: dict[str, Any]) -> None:
        """``LIVE_OPEN_PLATFORM_LIKE`` 事件处理：累加点赞总数。

        点赞事件每秒推一次，频率非常高。如果每条都转 envelope 会把 chatter
        拉满垃圾消息，所以这里只累加内部计数器，由
        :class:`plugin.BilibiliLiveAdapter` 周期性把当前总数注入 prompt 的
        ``system_reminder``。

        B 站 LIKE 推送里的 ``like_count`` 字段含义是"该用户本批次点赞数"
        而不是直播间累计；所以这里直接 ``+= like_count``（缺省按 1 计）。
        """

        try:
            increment = int(data.get("like_count") or 1)
        except (TypeError, ValueError):
            increment = 1
        if increment <= 0:
            increment = 1
        self._total_likes += increment
        logger.debug(
            f"点赞 +{increment} 累计={self._total_likes} "
            f"(uname={data.get('uname')})"
        )


__all__ = [
    "BilibiliDispatcher",
    "CMD_DM",
    "CMD_GIFT",
    "CMD_GUARD",
    "CMD_LIKE",
    "CMD_SUPER_CHAT",
    "PLATFORM",
]
