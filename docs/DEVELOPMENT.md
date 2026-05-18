# 开发流程与续作 checkpoint

> 本文档记录这个 Adapter 的设计决策、开发顺序、当前进度。
> 任何续作（包括我隔几天回来 / 别人接手）都从这一页开始读。

---

## 当前进度

| 阶段 | 内容 | 状态 |
|------|------|------|
| 1 | 目录结构 + manifest.json + README.md | ✅ |
| 2 | docs/API.md 完整协议归档 | ✅ |
| 3 | docs/DEVELOPMENT.md 开发流程文档（本文件） | ✅ |
| 4 | config.py 凭证配置 | ✅ |
| 5 | src/proto.py 二进制包打包/解包 | ✅ |
| 6 | src/api.py HTTP 签名 + start / heartbeat / end | ✅（签名已对官方 demo 校验通过） |
| 7 | src/client.py WebSocket 长连 + 双心跳 + recv loop + 解压 | ✅ |
| 8 | src/dispatcher.py cmd 路由 → MessageEnvelope（先 DM） | ✅ |
| 9 | plugin.py BilibiliLiveAdapter + Plugin | ✅（自管 ws，重写 health_check / reconnect） |
| 10 | 本地静态 + import 校验 | ✅（proto / dispatcher / plugin import / zlib 解压都已通过） |
| 11 | 用户填凭证实测 | ⬜ — 等开发者审核通过 + 填凭证 |

## 已通过的本地校验

下列校验都跑过、过：

- `proto.pack(op=heartbeat) → unpack` 还原一致
- `proto.pack(op=auth) → unpack` body 字符串原样还原
- `proto.iter_packets(multi)` 多包合并切包正确
- `proto.decompress(VER_ZLIB)` zlib 多包解压后再 `iter_packets`
- `dispatcher.dispatch(DM payload)` 输出 envelope 字段齐全（platform / user_info / group_info / additional_config）
- `dispatcher.dispatch(空 msg / unknown cmd / LIKE)` 全部返回 None
- `import plugins.bilibili_live_adapter.plugin` 全链路 OK
- HTTP 签名输出与官方文档 demo 字符串一致（`a81c50234b6bbf15bc56e387ee4f19c6f871af2f70b837dc56db16517d4a341f`）

下次续作如果协议层（proto / dispatcher / api）有改动，建议重跑这套断言再 commit。

---

## 设计决策

### 为什么不复用 mofox-wire 的 WebSocketAdapterOptions

像 napcat 那种走 `WebSocketAdapterOptions` + `super().__init__(transport=...)` 自动管 ws 的方式跑不通，因为：

1. B 站协议是**自定义二进制包**（4 字节 packetLen + 16 字节包头 + body），不是 JSON 文本帧
2. 需要**两套并行心跳**（WS op=2 + HTTP `/heartbeat`），mofox-wire 的传输抽象只管一个
3. 需要在 ws 收包后**做 zlib/brotli 解压**再切包，mofox-wire 不知道这事

所以本 Adapter **自己开 ws 连接、自己跑心跳、自己解码包**，完成后再调 `self.core_sink.send(envelope)` 把消息送进核心。

### 为什么 `_send_platform_message` 是 no-op

B 站直播开放平台**不允许第三方 bot 发弹幕**。回应通过 `anima_chatter` 走 TTS + VTube Studio。

如果以后 B 站放开了发弹幕的 API，再实现 `_send_platform_message`。

### 为什么用 httpx 而不是 requests

- requests 是同步的，会卡住事件循环
- httpx 异步、API 几乎一样、性能不差

### 为什么把 brotli 列为依赖

B 站 `ver=3` 包用 brotli 压缩。如果用户的房间永远不会推压缩包（小直播间几乎不会），brotli 不装也能跑。但写在 `python_dependencies` 里更稳，避免运行时才发现。

---

## 模块边界

### proto.py

**职责**：纯二进制包格式，无副作用。

```python
def pack(op: int, body: bytes = b"", seq: int = 1) -> bytes: ...
def unpack(buf: bytes) -> tuple[Header, bytes]: ...
def split_compressed(decompressed: bytes) -> Iterable[tuple[Header, bytes]]: ...
```

不依赖任何 Neo-MoFox 接口，可以单独跑测试。

### api.py

**职责**：HTTP 签名 + 三个接口的请求/响应封装。

```python
class BilibiliApi:
    async def start_app(self) -> StartResp: ...
    async def app_heartbeat(self, game_id: str) -> None: ...
    async def end_app(self, game_id: str) -> None: ...
```

签名逻辑见 `_sign(headers, body_str)`，对应 [`API.md` § 2.2`](API.md)。

### client.py

**职责**：WebSocket 长连 + 双心跳 + recv loop。**不知道业务事件长什么样**。

```python
class BilibiliClient:
    def __init__(self, api: BilibiliApi, on_event: Callable[[dict], Awaitable[None]]): ...
    async def start(self): ...   # 建立长连，启动心跳和 recv 任务
    async def stop(self): ...
```

收到 `op=5` 包时反序列化 body 然后调 `on_event(payload)`。`on_event` 由 dispatcher 传入。

### dispatcher.py

**职责**：把 `op=5` body 路由到 `MessageEnvelope`。

```python
async def dispatch(payload: dict) -> MessageEnvelope | None: ...
```

第一版只处理 `LIVE_OPEN_PLATFORM_DM`，其他 cmd return None。后续扩展时往这里加 case。

### plugin.py

**职责**：把上面四个模块粘起来，实现 `BaseAdapter` 协议。

- `on_adapter_loaded` → 创建 api / client / dispatcher
- `start` → `client.start()`
- `stop` / `on_adapter_unloaded` → `client.stop()` + `api.end_app()`
- `from_platform_message` → 实际不被调用（因为我们自己管收包）；保留空实现满足基类
- `_send_platform_message` → no-op

---

## 续作 checkpoint

### 我（或者下一个人）从断点回来时怎么续

1. **先读 README.md** —— 整体能力图
2. **再读 API.md** —— 协议细节
3. **看本文件 "当前进度"** —— 知道下一步该做什么
4. **看 git log** —— 了解最近改了什么
5. **跑一遍静态校验** —— 确保还能 import：
   ```bash
   uv run python -c "import plugins.bilibili_live_adapter.plugin; print('OK')"
   ```

### 扩展事件（礼物 / SC / 上舰）

第一版只跑 DM。要加礼物 / SC / 上舰：

1. 看 [`API.md § 5`](API.md) 找对应 cmd 字段格式
2. 在 `dispatcher.py` 加新 case，转成对应 `MessageEnvelope`
3. 决定 `message_segment.type` —— 第一版 DM 就是 `text`；礼物可能用扩展类型，比如：
   - 礼物：`message_segment = {"type":"bilibili_gift","data":{...}}`，下游 chatter 自己处理
   - 或者全部转成 `text` 拼接：`"用户A 投喂了 1 个 小心心"`，简单但丢失结构
4. 在 `anima_chatter` 这边加 chatter / action 处理这些事件

### 出站（如果以后 B 站允许 bot 发弹幕）

1. 在 `api.py` 加 `send_dm(room_id, content)` 调用对应接口
2. 在 `plugin.py` 的 `_send_platform_message` 解析 envelope.message_segment，提取文本，调 `api.send_dm`

---

## 常见坑（按踩坑顺序更新）

> 每次踩到新坑就在这里加一行，方便下一个人少走弯路。

- *暂无*

---

## 测试方法

### 1. 静态校验（不需要凭证）

```bash
uv run python -c "import plugins.bilibili_live_adapter.plugin; print('OK')"
```

### 2. 单元测试 proto.py（不需要凭证）

```python
from plugins.bilibili_live_adapter.src.proto import pack, unpack
buf = pack(op=2, body=b"")  # 心跳包
header, body = unpack(buf)
assert header.op == 2
assert body == b""
```

### 3. 集成测试（需要真实凭证）

填 `config/plugins/bilibili_live_adapter/config.toml` 然后：

```bash
uv run main.py
```

观察日志里有没有：

- `连接 B 站长连：wss://...` ← 长连建立
- `B 站鉴权成功` ← op=8 收到 code=0
- `心跳循环已启动` ← 心跳开始
- 弹幕事件 ← 真直播间有人发弹幕

### 4. 调试关键 log key

为了方便排错，关键路径都用 logger.info 打点：

| log key | 含义 |
|--------|------|
| `bilibili_live_adapter.api.start` | /v2/app/start 请求 / 响应 |
| `bilibili_live_adapter.client.connected` | ws 长连建立 |
| `bilibili_live_adapter.client.auth.ok` | 鉴权成功 |
| `bilibili_live_adapter.client.heartbeat.ws` | ws 心跳 |
| `bilibili_live_adapter.client.heartbeat.app` | http 心跳 |
| `bilibili_live_adapter.dispatcher.dm` | 收到弹幕 |
| `bilibili_live_adapter.client.disconnect` | 长连断开 |
