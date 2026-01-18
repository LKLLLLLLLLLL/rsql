# WebSocket 代理架构说明

## 架构变更

将 web_server 改造为双 WebSocket 连接架构。

### 修改前的架构 ❌
```
前端 <--WebSocket--> 4456端口后端（直接处理SQL）
```

### 修改后的架构 ✅
```
前端 <--WebSocket--> 4456端口后端 <--WebSocket--> 4455端口服务器
      连接1                        连接2（转发）
```

## 数据流

1. **前端 → 4456端口**：发送 SQL 查询请求
   - 格式：`RayonQueryRequest` (JSON)
   - 包含：username, userid, request_content

2. **4456端口 → 4455端口**：转发查询请求到后端服务器
   - 使用 awc WebSocket 客户端
   - 同样发送 `RayonQueryRequest` 格式

3. **4455端口 → 4456端口**：返回查询结果
   - 格式：`WebsocketResponse` (JSON)
   - 包含：rayon_response, timestamp, success, connection_id

4. **4456端口 → 前端**：返回最终结果
   - 提取 `response_content` 字段
   - 封装为 `WebsocketResponse` 返回前端

## 修改的文件

### 1. Cargo.toml
- 添加 `awc = "3.5.2"` - WebSocket 客户端库
- 添加 `actix-codec = "0.5.2"` - 编解码支持
- 添加 `actix-http = "3.10.2"` - HTTP 协议支持
- 启用 `tokio` 的 `sync` feature

### 2. web_server.rs
- 添加 `backend_client: Arc<Client>` 到 `AppState`
- 创建 WebSocket 客户端实例
- 将客户端传递给 `WebsocketActor`

### 3. websocket_actor.rs
- 添加 `backend_client: Arc<Client>` 字段
- 实现 `forward_to_backend()` 函数，转发请求到 4455 端口
- 修改请求处理逻辑：从直接调用 `working_thread_pool` 改为调用 `forward_to_backend()`

## 关键函数

### `forward_to_backend()` 
位于 `websocket_actor.rs` 末尾，负责：
1. 连接到 `ws://127.0.0.1:4455/ws`
2. 发送 JSON 格式的查询请求
3. 接收 JSON 格式的响应
4. 解析并返回结果

## 测试要求

确保 4455 端口的服务器：
1. 监听 `/ws` 路由
2. 接受 `RayonQueryRequest` 格式的请求
3. 返回 `WebsocketResponse` 格式的响应

## 注意事项

- 4456 端口仍然接收前端连接（保持兼容）
- 数据格式完全按照 `types.rs` 定义
- 转发过程对前端透明
- 错误处理：如果 4455 端口连接失败，会返回错误消息给前端
