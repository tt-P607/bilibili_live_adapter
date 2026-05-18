# Bilibili Live Adapter for Neo-MoFox

B 站直播开放平台弹幕入站适配器。

> ## 来源
>
> 本插件由 [言柒](https://github.com/tt-P607) 基于 B 站直播开放平台协议自研开发，旨在为 Neo-MoFox 框架提供原生的直播互动能力。
> 
> 仓库地址：[tt-P607/bilibili_live_adapter](https://github.com/tt-P607/bilibili_live_adapter)

## 功能特性

- **纯入站设计**：由于 B 站直播开放平台不允许第三方 Bot 发送弹幕，本适配器仅负责接收弹幕、礼物、SC 等事件并转换为 Neo-MoFox 的统一消息模型。
- **VTB 联动**：建议配合 [`anima_chatter`](../anima_chatter/) 插件使用，实现"观众发弹幕 -> Bot 语音回复 + 形象表演"的完整直播互动链路。
- **二进制协议实现**：自研 B 站直播 WebSocket 二进制协议解码（pktLen + 包头 + Body），支持 zlib/brotli 解压，内置双心跳机制。
- **身份映射**：
  - `user_id` 映射为 B 站脱敏 `open_id`（跨直播间稳定）；
  - `group_id` 映射为直播间号；
  - 舰长/提督/总督自动映射为 `OPERATOR` 角色。

## 快速上手

### 1. 申请凭证
1. 登录 [B 站直播开放平台](https://open-live.bilibili.com/)；
2. 创建项目并获取以下四项关键信息：
   - `access_key_id`
   - `access_key_secret`
   - `app_id` (项目 ID)
   - `room_id` (你要挂载的直播间号)

### 2. 配置插件
在 `config/plugins/bilibili_live_adapter/config.toml` 中填入凭证：

```toml
[plugin]
enabled = true

[auth]
access_key_id = "你的ID"
access_key_secret = "你的Secret"
app_id = 1234567890123  # 你的项目ID
room_id = 123456        # 目标直播间号
```

### 3. 运行
启动 Neo-MoFox 即可。适配器会自动建立连接并开始监听弹幕。

## 设计决策

### 为什么不复用 mofox-wire 的 WebSocketAdapterOptions
像 napcat 那种自动管 ws 的方式跑不通，因为：
1. B 站协议是**自定义二进制包**（4 字节 packetLen + 16 字节包头 + body），不是 JSON 文本帧；
2. 需要**两套并行心跳**（WS op=2 + HTTP `/heartbeat`），mofox-wire 的传输抽象只管一个；
3. 需要在 ws 收包后**做 zlib/brotli 解压**再切包。

因此本 Adapter **自己开 ws 连接、自己跑心跳、自己解码包**，完成后再调 `self.core_sink.send(envelope)` 把消息送进核心。

### 为什么 `_send_platform_message` 是 no-op
B 站直播开放平台**不允许第三方 bot 发弹幕**。回应通过 `anima_chatter` 走 TTS + VTube Studio。如果以后 B 站放开了发弹幕的 API，再实现此方法。

## 开发者说明

- **协议文档**：详见 [`docs/API.md`](docs/API.md)；
- **开发流程**：详见 [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)；
- **依赖**：`httpx`, `brotli` (已声明在 manifest)。

## 许可证

[AGPL-v3.0](LICENSE)
