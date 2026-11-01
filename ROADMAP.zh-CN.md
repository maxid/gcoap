[English](ROADMAP.md) | 简体中文

# 路线图

- [x] 基于 gnety 的 UDP 服务端
- [x] CoAP 请求路由（GET/POST/PUT/DELETE）
- [x] CON/ACK 与 NON 响应
- [x] Query/URI-path 解析
- [x] Observe / 通知（最小实现）
- [x] 块传输（Block1/Block2）
- [x] DTLS 传输（服务端/客户端）
- [x] 客户端状态机（最小：Dial/Do、重试、Block2）
- [x] RFC 7252 重传、message-id 去重与 exchange lifetime
- [x] Confirmable Observe 通知：ACK/RST 处理与有界 per-observation 队列
- [x] Separate response（空 ACK 后发送重传的 CON 响应）
- [x] 客户端 Block1 上传（大小协商、Size1、服务端提前响应）
- [x] 客户端 Observe（序列跟踪、NON/CON 通知、ACK、取消）
