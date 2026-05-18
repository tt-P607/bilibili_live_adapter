# B 站直播开放平台 协议与 API 速查

> **来源**：[官方文档](https://open-live.bilibili.com/document/bdb1a8e5-a675-5bfe-41a9-7a7163f75dbf) + 官方 Python demo（`240311-py-demo-new`）+ 用户提供的鉴权章节
>
> **目的**：本插件复用、续作、排错时可以独立查阅，不依赖外部网页（防止官方文档移动 / 改版）。

---

## 1. 环境域名

| 环境 | 域名 |
|------|------|
| 正式 | `https://live-open.biliapi.com` |

注意事项：

- 所有 HTTP 请求 **method 必须为 POST**，不要在 URL 拼接 GET 参数
- `Content-Type` 必须为 `application/json`
- 时间戳允许误差 **10 分钟**，超出会被丢弃为 `4003`
- 单个直播间对同一应用最多同时打开 **5 个连接**（超过返回 `7010`）

---

## 2. HTTP 鉴权（签名机制）

### 2.1 公共请求头

| 头名 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `Accept` | string | 是 | `application/json` |
| `Content-Type` | string | 是 | `application/json` |
| `x-bili-content-md5` | string | 是 | **请求体 JSON 字符串**的 MD5（小写 hex） |
| `x-bili-timestamp` | string | 是 | UNIX 时间戳（秒），与服务端误差 ≤10 分钟 |
| `x-bili-signature-method` | string | 是 | 固定 `HMAC-SHA256` |
| `x-bili-signature-nonce` | string | 是 | 全网唯一随机串，建议 UUID |
| `x-bili-accesskeyid` | string | 是 | 你申请到的 AccessKey ID |
| `x-bili-signature-version` | string | 是 | 固定 `1.0` |
| `Authorization` | string | 是 | 计算出来的签名（小写 hex） |

### 2.2 待签名字符串构造

把 6 个 `x-bili-` 头按**字典序**升序，每行 `key:value`，行间 `\n`，**第一行开头不加换行，最后一行结尾不加换行**：

```
x-bili-accesskeyid:$accessKeyId
x-bili-content-md5:$contentMd5
x-bili-signature-method:HMAC-SHA256
x-bili-signature-nonce:$nonce
x-bili-signature-version:1.0
x-bili-timestamp:$timestamp
```

### 2.3 签名计算

```python
import hmac
from hashlib import sha256

signature = hmac.new(
    access_key_secret.encode(),
    header_str.encode(),
    digestmod=sha256,
).hexdigest()
```

**注意**：签名结果必须是**小写**十六进制字符串。`Authorization` 头的值就是它。

### 2.4 通用响应

所有接口 HTTP status 都是 200，业务结果看 body：

```json
{
  "code": 0,
  "message": "成功",
  "request_id": "1409787829088702464",
  "data": { ... }
}
```

`code != 0` 即为业务错误，错误码见第 6 节。

---

## 3. 三个核心 HTTP 接口

### 3.1 启动应用 `POST /v2/app/start`

请求体：

```json
{
  "code": "<id_code>",
  "app_id": <app_id 整数>
}
```

返回：

```json
{
  "code": 0,
  "data": {
    "game_info": {
      "game_id": "<gameId>"
    },
    "websocket_info": {
      "auth_body": "<auth_body 字符串>",
      "wss_link": ["<wss_url1>", "<wss_url2>"]
    },
    "anchor_info": {
      "uid": 0,
      "uname": "...",
      "uface": "...",
      "room_id": 0
    }
  }
}
```

**关键产出**：

- `game_id` — 后续 `/heartbeat` 和 `/end` 都要用
- `wss_link[0]` — 长连地址（demo 取第一个；可以做容灾备选）
- `auth_body` — 鉴权信息，第一帧 op=7 包的 body

### 3.2 应用心跳 `POST /v2/app/heartbeat`

请求体：

```json
{ "game_id": "<gameId>" }
```

每 **20 秒**调一次。漏调超时后 `game_id` 失效（`7003`）。

### 3.3 关闭应用 `POST /v2/app/end`

请求体：

```json
{ "game_id": "<gameId>", "app_id": <app_id> }
```

插件 `on_adapter_unloaded` 时调用，释放 game_id 占用。

---

## 4. WebSocket 长连协议

### 4.1 包结构（16 字节包头 + body）

| 偏移 | 长度 | 字段 | 含义 |
|------|------|------|------|
| 0 | 4 | packetLen | 整包长度（含包头），大端 int |
| 4 | 2 | headerLen | 包头长度，固定 16，大端 short |
| 6 | 2 | ver | 协议版本（见 4.2） |
| 8 | 4 | op | 操作码（见 4.3） |
| 12 | 4 | seq | 序列号，大端 int |
| 16 | packetLen-16 | body | 数据 body（json / 多包合并 / brotli 压缩等） |

### 4.2 协议版本 ver

| ver | 含义 |
|-----|------|
| 0 | body 为 JSON 文本 |
| 1 | 操作码相关无业务 body（如心跳） |
| 2 | body 为 zlib 压缩的多包合并 |
| 3 | body 为 brotli 压缩的多包合并 |

第一版优先支持 0 / 1。压缩包（2/3）需要解压再切包，每个内层包再次走 unpack 流程。

### 4.3 操作码 op

| op | 方向 | 含义 |
|----|------|------|
| 2 | client → server | WebSocket 心跳（无 body） |
| 3 | server → client | WebSocket 心跳回包（带在线人数等） |
| 5 | server → client | 业务推送（弹幕、礼物、SC、上舰…） |
| 7 | client → server | 鉴权（body = `auth_body` 字符串） |
| 8 | server → client | 鉴权回包（body = `{"code":0}` 表示成功） |

### 4.4 鉴权 + 心跳时序

```
1. WebSocket 连接到 wss_link[0]
2. 发 op=7 包：body = auth_body 字符串
3. 收 op=8 包：解析 body，code == 0 才算成功
4. 启动两个并发循环：
   - 每 20s 发 op=2 包（无 body，纯包头）
   - 每 20s 发一次 HTTP /v2/app/heartbeat
5. recv loop 持续读 op=5 包，按 cmd 字段路由业务事件
```

漏掉**任何一个**心跳都会被服务端断连——平台用应用心跳判 game_id 有效，用 WS 心跳判长连活跃。

---

## 5. 业务推送（op=5）的 body 格式

`op=5` 包 body 是 JSON 字符串。结构：

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_DM",
  "data": { ... 具体事件数据 ... }
}
```

### 5.1 弹幕 `LIVE_OPEN_PLATFORM_DM`

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_DM",
  "data": {
    "uname": "用户昵称",
    "uid": 123456,
    "open_id": "open_xxx",        // 平台脱敏 ID（推荐使用，比 uid 稳定）
    "uface": "https://...",
    "msg": "弹幕内容",
    "msg_id": "...",
    "fans_medal_level": 0,
    "fans_medal_name": "",
    "fans_medal_wearing_status": false,
    "guard_level": 0,             // 0=非舰长 1=总督 2=提督 3=舰长
    "timestamp": 1700000000,
    "room_id": 12345,
    "emoji_img_url": "",          // 表情包弹幕的图片 URL
    "dm_type": 0                  // 0=普通文本 1=表情包
  }
}
```

### 5.2 礼物 `LIVE_OPEN_PLATFORM_SEND_GIFT`

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_SEND_GIFT",
  "data": {
    "uname": "...",
    "uid": 0,
    "open_id": "open_xxx",
    "uface": "...",
    "gift_id": 0,
    "gift_name": "...",
    "gift_num": 1,
    "price": 0,                   // 礼物总价（金瓜子，1元 = 1000 金瓜子）
    "paid": false,                // 是否付费礼物
    "fans_medal_level": 0,
    "fans_medal_name": "",
    "fans_medal_wearing_status": false,
    "guard_level": 0,
    "timestamp": 0,
    "anchor_info": { ... },
    "msg_id": "...",
    "gift_icon": "...",
    "combo_gift": false,
    "combo_info": null
  }
}
```

### 5.3 SC `LIVE_OPEN_PLATFORM_SUPER_CHAT`

字段类似礼物，关键字段：

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_SUPER_CHAT",
  "data": {
    "uname": "...", "open_id": "...", "uid": 0, "uface": "...",
    "message": "...",
    "msg_id": "...",
    "rmb": 0,                     // 人民币（元）
    "start_time": 0,              // SC 开始时间戳
    "end_time": 0                 // SC 结束时间戳
  }
}
```

### 5.4 上舰 `LIVE_OPEN_PLATFORM_GUARD`

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_GUARD",
  "data": {
    "user_info": { "uname": "...", "uid": 0, "open_id": "...", "uface": "..." },
    "guard_level": 1,             // 1=总督 2=提督 3=舰长
    "guard_num": 1,
    "guard_unit": "月",
    "fans_medal_level": 0,
    "fans_medal_name": "",
    "fans_medal_wearing_status": false,
    "timestamp": 0,
    "msg_id": "..."
  }
}
```

### 5.5 点赞 `LIVE_OPEN_PLATFORM_LIKE`

每秒推一次的累积点赞数。第一版**不处理**——意义不大且非常吵。

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_LIKE",
  "data": { "uname": "...", "uid": 0, "open_id": "...", "uface": "...", "like_text": "为主播点赞了", "like_count": 1, "fans_medal_level": 0, "fans_medal_name": "", "fans_medal_wearing_status": false, "timestamp": 0, "msg_id": "..." }
}
```

---

## 6. 错误码全集

### 6.1 通用错误（4xxx / 5xxx）

| 错误代码 | 描述 | 处理建议 |
|---------|------|---------|
| 4000 | 参数错误 | 检查必填参数与大小限制 |
| 4001 | 应用无效 | 检查 `x-bili-accesskeyid` 是否非空且有效 |
| 4002 | 签名异常 | 检查 `Authorization`（最常见：换行符多余、key 大小写错） |
| 4003 | 请求过期 | 同步系统时间，确保和服务端误差 < 10 分钟 |
| 4004 | 重复请求 | 检查 `x-bili-signature-nonce` 是否每次都是新 UUID |
| 4005 | 签名 method 异常 | 必须固定 `HMAC-SHA256` |
| 4006 | 版本异常 | 必须固定 `1.0` |
| 4007 | IP 白名单限制 | 确认请求 IP 已在开放平台后台报备 |
| 4008 | 权限异常 | 确认应用接口权限 |
| 4009 | 接口访问限制 | 检查频率（默认每秒不要超过 X 次） |
| 4010 | 接口不存在 | 检查 URL |
| 4011 | Content-Type 不为 `application/json` | — |
| 4012 | MD5 校验失败 | `x-bili-content-md5` 计算错误，注意要算请求体字符串而不是 dict |
| 4013 | Accept 不为 `application/json` | — |
| 5000 | 服务异常 | 联系 B 站对接同学 |
| 5001 | 请求超时 | 重试 |
| 5002 | 内部错误 | 联系 B 站对接同学 |
| 5003 | 配置错误 | 联系 B 站对接同学 |
| 5004 | 房间白名单限制 | 未上架应用只能连开发者本人房间 |
| 5005 | 房间黑名单限制 | 联系 B 站对接同学 |
| 5011 | 应用权限限制 | 默认未上架应用只允许开发者本人连接 |

### 6.2 验证码相关（6xxx）

| 错误代码 | 描述 |
|---------|------|
| 6000 | 验证码错误 |
| 6001 | 手机号码错误 |
| 6002 | 验证码已过期 |
| 6003 | 验证码频率限制 |
| 6010 | 房间号不能为空 |
| 6011 | 没有查询到房间 |
| 6012 | 主播信息为空 |
| 6013 | 互玩游戏关闭失败 |
| 6014 | 插件关闭失败 |
| 6015 | 直播工具关闭失败 |

### 6.3 互动游戏相关（7xxx）

| 错误代码 | 描述 |
|---------|------|
| 7000 | 不在游戏内 |
| 7001 | 请求冷却期，建议 10s 后重试 |
| 7002 | 房间重复游戏 |
| 7003 | 心跳过期 — game_id 已失效 |
| 7004 | 批量心跳超过最大值（200） |
| 7005 | 批量心跳 ID 重复 |
| 7007 | 身份码错误 — 主播身份码可能过期，去直播姬刷新 |
| 7008 | 插件重复开启 |
| 7009 | 无道具投放权限 |
| 7010 | 超过上限：单房间同应用最多 5 连接 |

### 6.4 其它

| 错误代码 | 描述 |
|---------|------|
| 8002 | 项目无权限访问，确认项目 ID |

---

## 7. 实现要点（写代码时回头查）

1. **md5 是请求体字符串的 md5，不是 dict 的 md5**——用 `json.dumps(...)` 拿到的字符串再算
2. **签名头按字典序拼**——key 按 ASCII 升序就是字典序
3. **wss_link 是数组**，第一版取 `wss_link[0]`，未来想做容灾可以遍历
4. **op=2 心跳没有 body**，包总长就是 16 字节
5. **op=7 鉴权包的 body 是 `auth_body` 字符串本身**，不要再 json.dumps 一层
6. **op=5 包的 body 反序列化后顶层是 `{"cmd":"...","data":{...}}`**，不要直接当 data 用
7. **失败 4003** 大概率是时钟漂移，检查系统 NTP
8. **断开重连**：拿到新的 `wss_link` + `auth_body`（重新调 /start）→ 重新走鉴权 → 重启心跳
