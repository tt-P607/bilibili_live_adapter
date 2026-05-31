# Bilibili Live Adapter for Neo-MoFox

B 站直播开放平台弹幕入站适配器。

> ## 来源
>
> 本插件由 [言柒](https://github.com/tt-P607) 基于 B 站直播开放平台协议自研开发，旨在为 Neo-MoFox 框架提供原生的直播互动能力。
>
> 仓库地址：[tt-P607/bilibili_live_adapter](https://github.com/tt-P607/bilibili_live_adapter)

## 功能特性

- **纯入站设计**：B 站直播开放平台不允许第三方 Bot 发送弹幕，本适配器仅负责接收弹幕、礼物、SC、上舰等事件并转换为 Neo-MoFox 的统一消息模型。
- **VTB 联动**：建议配合 [`anima_chatter`](../anima_chatter/) 使用，实现"观众发弹幕 → Bot 语音回复 + 形象表演"的完整直播互动链路。
- **二进制协议**：自研 B 站直播 WebSocket 二进制协议解码（pktLen + 包头 + Body），支持 zlib/brotli 解压，内置双心跳（WS op=2 + HTTP `/heartbeat`）。
- **身份映射**：
  - `user_id` → B 站脱敏 `open_id`（跨直播间稳定）；
  - 舰长 / 提督 / 总督自动映射为 `OPERATOR` 角色。
- **指数退避重连** + `game_id` 持久化（防止 7002 房间重复游戏限流）。

## 与多平台直播协同

本插件与 [`douyin_live_adapter`](../douyin_live_adapter/) 共享一套"合并 stream"约定，
让 B 站 + 抖音同时直播时能进入**同一个聊天会话**，由 anima_chatter 串行决策、避免两边
chatter / VTS 打架：

- envelope 的 `platform` 统一为 `"live"`，`group_id` 统一为 `"live_room"`，因此两个平台的
  弹幕计算出来的 `stream_id` 完全相同。
- 真实来源由 `additional_config.source_platform`（也透传到 `Message.extra`）携带，
  本插件这里永远是 `"bilibili_live"`。
- 真实直播间号由 `additional_config.source_room_id` 携带。

## 快速上手

### 1. 申请凭证

1. 登录 [B 站直播开放平台](https://open-live.bilibili.com/)；
2. 创建项目并获取以下信息：
   - `access_key_id`
   - `access_key_secret`
   - `app_id`（项目 ID）
   - `id_code`（主播身份码，主播在 B 站直播姬"主播专用"页面查看）

### 2. 配置插件

在 `config/plugins/bilibili_live_adapter/config.toml` 填：

```toml
[bilibili]
access_key_id = "你的 ID"
access_key_secret = "你的 Secret"
app_id = 1234567890123
id_code = "主播身份码"

# 聊天流（直播间）的显示名；留空则用 "B 站直播间 {room_id}" 兜底。
# 多平台同播时建议两个 adapter 填同一个值，stream 才能用统一名字。
stream_name = ""

[connection]
heartbeat_ws_interval = 20.0     # WebSocket op=2 心跳间隔
heartbeat_app_interval = 20.0    # HTTP /heartbeat 间隔
auto_reconnect = true
reconnect_initial_delay = 2.0
reconnect_max_delay = 60.0
request_timeout = 15.0
```

### 3. 启动 Bot

```bash
uv run main.py
```

适配器会自动调 `/v2/app/start` 拿 `wss_link` + `auth_body`，建立长连并开始监听弹幕。

## 平台标识

| 字段 | 值 |
|------|------|
| envelope `platform` | `"live"`（合并 stream 用的虚拟平台名） |
| envelope `group_id` | `"live_room"`（合并 stream 用的虚拟群 ID） |
| envelope `group_name` | 配置 `stream_name` 优先；留空时 `"B 站直播间 {room_id}"` |
| `source_platform` | `"bilibili_live"`（注入到 `additional_config` 与 `message_info.extra`） |
| 用户标识 | `open_id`（开放平台脱敏 ID，跨直播间稳定） |

## 设计决策

### 为什么不复用 mofox-wire 的 `WebSocketAdapterOptions`

像 napcat 那种自动管 ws 的方式跑不通：

1. B 站协议是**自定义二进制包**（4 字节 packetLen + 16 字节包头 + body），不是 JSON 文本帧；
2. 需要**两套并行心跳**（WS op=2 + HTTP `/heartbeat`），mofox-wire 的传输抽象只管一个；
3. 需要在 ws 收包后**做 zlib/brotli 解压**再切包。

因此本 Adapter **自己开 ws、自己跑心跳、自己解码包**，完成后再调 `self.core_sink.send(envelope)` 把消息送进核心。

### 为什么 `_send_platform_message` 是 no-op

B 站直播开放平台**不允许第三方 bot 发弹幕**。回应通过 `anima_chatter` 走 TTS + VTube Studio。

## 协同插件

- 与 [`anima_chatter`](../anima_chatter/) 联动可实现"弹幕进 → TTS 出 → VTube 形象动起来"。
- 与 [`douyin_live_adapter`](../douyin_live_adapter/) 完全独立，可同时启用并自动合并 stream。
- 任何消费 `MessageEnvelope` 的下游插件都能按 `additional_config.source_platform` 区分本插件来源。

## 开发者说明

- 协议详情：[`docs/API.md`](docs/API.md)
- 开发流程：[`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)
- 依赖：`httpx`, `brotli`（已声明在 `manifest.json`）

## 许可证

[AGPL-v3.0](LICENSE)
