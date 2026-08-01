"""bilibili_live_adapter 插件配置。

四个核心凭证从 B 站开放平台后台获取：

- ``access_key_id`` / ``access_key_secret``：开放平台 → 应用管理 → 创建应用
- ``app_id``：同上，创建后生成的整数 ID
- ``id_code``：主播身份码，主播在 bilibili 直播姬"主播专用"页面查看

详见 :mod:`docs.API` 第 1 节。
"""

from __future__ import annotations

from typing import ClassVar

from src.core.components.base.config import BaseConfig, Field, SectionBase, config_section


class BilibiliLiveAdapterConfig(BaseConfig):
    """B 站直播弹幕 Adapter 配置。"""

    name: ClassVar[str] = "config"
    description: ClassVar[str] = (
        "bilibili_live_adapter 插件配置（开放平台凭证 + 长连参数）"
    )

    @config_section("plugin", title="插件总开关")
    class PluginSection(SectionBase):
        """插件级开关。

        关闭后插件本身仍然会被框架加载（adapter 也会被注册），但不会去调
        ``/v2/app/start``、不会建立 WebSocket 长连——相当于"占位但不工作"。
        想完全卸载请改 ``manifest.json`` 或移除插件目录。
        """

        enabled: bool = Field(
            default=True,
            description="是否启用本插件的长连功能；关闭后不会建立任何 B 站直播 WS 长连",
        )

    @config_section("bilibili", title="B 站开放平台凭证")
    class BilibiliSection(SectionBase):
        """开放平台凭证 + 主播身份码。

        所有字段从 https://open-live.bilibili.com 后台获取，**不要提交到公共仓库**。
        如果用 `git status` 时配置文件本身被追踪了，请加进 `.gitignore`。
        """

        access_key_id: str = Field(
            default="",
            description="AccessKey ID，HTTP 签名头 x-bili-accesskeyid 的值",
        )
        access_key_secret: str = Field(
            default="",
            description="AccessKey Secret，HMAC-SHA256 签名密钥（务必保密）",
        )
        app_id: int = Field(
            default=0,
            description="应用 ID（整数），开放平台后台创建应用后生成",
        )
        id_code: str = Field(
            default="",
            description=(
                "主播身份码。主播登录 bilibili 直播姬，在'主播专用'页面查看。"
                "可能会过期，过期会报 7007，去直播姬重新刷新即可。"
            ),
        )
        host: str = Field(
            default="https://live-open.biliapi.com",
            description="API 主机域名。正式环境就是默认值，不要乱改。",
        )
        stream_name: str = Field(
            default="",
            description=(
                "聊天流（直播间）的显示名；留空则用 ``\"B 站直播间 {room_id}\"`` 兜底。"
                "多平台同播时建议两个 adapter 填同一个值，stream 才能用统一名字。"
            ),
        )

    @config_section("connection", title="长连参数")
    class ConnectionSection(SectionBase):
        """WebSocket 长连接相关参数。

        默认值参考 B 站官方 demo：心跳都是 20 秒一次。
        改长心跳间隔会被服务端断开，改短没意义。
        """

        heartbeat_ws_interval: float = Field(
            default=20.0,
            description="WebSocket op=2 心跳间隔（秒），保持长连活跃",
        )
        heartbeat_app_interval: float = Field(
            default=20.0,
            description="HTTP /v2/app/heartbeat 间隔（秒），保持 game_id 有效",
        )
        auto_reconnect: bool = Field(
            default=True,
            description=(
                "长连断开后是否自动重连（重新调 /start 拿新的 wss_link 和 auth_body）。"
                "开发期建议保持 true。"
            ),
        )
        reconnect_initial_delay: float = Field(
            default=2.0,
            description="重连首次延迟（秒）；连续失败会指数退避",
        )
        reconnect_max_delay: float = Field(
            default=60.0,
            description="重连退避封顶（秒），避免 B 站故障期间日志刷爆",
        )
        request_timeout: float = Field(
            default=15.0,
            description="HTTP 请求超时（秒）",
        )

    plugin: PluginSection = Field(default_factory=PluginSection)
    bilibili: BilibiliSection = Field(default_factory=BilibiliSection)
    connection: ConnectionSection = Field(default_factory=ConnectionSection)


__all__ = ["BilibiliLiveAdapterConfig"]
