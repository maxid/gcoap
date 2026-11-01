[English](CHANGELOG.md) | 简体中文

# 变更日志

> 开发快照不打 tag；以下条目按时间顺序排列。

## [Unreleased]

- 基于 gnety UDP 的最小 CoAP 服务端：路由、CON/ACK 与 NON 响应、query/path 解析、4.04 处理。
- Observe/Notify：注册（Observe=0）、通知、取消（Observe=1）。
- 块传输：Block2 响应切片与 Block1 上传拼装。
- 基于 gnety client connector 的客户端（`Dial`/`Do`）：CON 重试、token 匹配、Block2 拼装。
- 通过 gnety 配置 DTLS 服务端/客户端。
- RFC 7252 exchange 层（C1）：入站 confirmable 请求按 peer identity + message id 在有限缓存（`MaxExchanges`、exchange lifetime）中去重；重传的 CON 逐字节重放缓存响应，绝不重跑 handler；NON 请求不去重。
- 出站 confirmable 重传：指数退避、ACK_RANDOM_FACTOR 抖动、MAX_RETRANSMIT 与 EXCHANGE_LIFETIME；收到对应 message id 的 ACK/RST 结束 exchange。客户端使用该调度，不再固定 1500ms/两次重试；空 ACK 停止重传，peer reset 返回新的 `ErrReset`。
- exchange 完成、session 关闭或服务端停止时取消 handler context，不再使用 `context.Background()`。
- `Config`/`ClientConfig` 增加 RFC 7252 时间字段（`AckTimeout`、`AckRandomFactor`、`MaxRetransmit`、`ExchangeLifetime`）；`Config` 增加 `MaxExchanges`；默认仍为 2s、1.5、4、247s、1024。
- Options、blocks 与 observe 值通过 `gnety/codec/coap` 的类型化访问器编解码；gcoap 保留 exchange、observe registry、block transfer 状态及紧凑 Block/三字节 Observe 线格式。
- Confirmable 通知（C2）：`Config.ConfirmableNotifications` 以 CON 发送 Observe 通知，并按 RFC 7252 调度重传直到 ACK/RST 或重传上限；RST 与最终超时移除 observation（RFC 7641 §4.5）；ACK 最多释放一条合并的新通知，因此每个 observation 只有一条 in-flight + 一条 pending，与 `Notify` 频率无关。默认仍为 NON。
- Separate response（C3）：confirmable handler 超过 `ACK_TIMEOUT` 时先发空 ACK，再以新的 CON 消息（独立 message id 与重传调度）发送响应。去重缓存向重复请求重放空 ACK（绝不作为最终答案），并保证 handler 只执行一次；`Request.RespondLater` 显式暴露同一路径；`Config.SeparateResponses` 把 handler 移出 transport read loop，使重复请求可在执行期间得到应答。客户端会 ACK separate CON 响应，服务端因此停止重传。
- 客户端 Block1（C3）：`Do` 把大于 `ClientConfig.BlockSize`（默认 1024，16–1024 的 2 的幂）的请求体拆成 Block1 序列，而不是单个 datagram。每个非最终块必须以 2.31 Continue 应答；应答中更小的 SZX 会降低剩余块大小（更大的忽略），按新大小重算块号且不跳字节；`Size1` 携带总大小，服务端返回小于 body 的 `Size1` 会停止上传，`ClientConfig.MaxRequestSize`（默认 1 MiB）在发送前用新的 `ErrRequestTooLarge` 拒绝超大 body；非最终块上的早期错误或最终响应会结束上传并返回调用方。
- 客户端 Observe（C3）：`Client.Observe` 以 Observe=0 注册，返回注册响应与 `Observation`；通知通过回调到达。每个 observation 的序列号遵循 RFC 7641 §3.4，过期通知被丢弃，NON 通知可接受，CON 通知会被 ACK（过期的也 ACK 以停止重传）。observation 在 context 取消、`Observation.Cancel`（confirmable 注销）或错误通知时结束；不留下 goroutine/map entry，`Client.Close` 结束所有 observation。
- 客户端现在由单个 goroutine 读取连接，并按 token/message id 路由 datagram，因此 observation 与请求可同时 in flight；`Do` 仍串行。可观察行为变化：超过 BlockSize 的请求体按块发送，超过 MaxRequestSize 的 body 被拒绝，Block2 后续请求不再重复 body，`Close` 后读失败报告为 `ErrClientClosed`。
- 服务端对 POST 与 PUT 都重组 Block1 请求体，客户端 POST 上传路径需要这一点。
