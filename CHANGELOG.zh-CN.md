[English](CHANGELOG.md) | 简体中文

# 变更日志

> 开发快照不打 tag；以下条目按时间顺序排列。

## [Unreleased]

### 变更

- **指标契约移到 `gcoap/hook`。** `Metrics`、`NoopMetrics` 与全部标签（`Result*`、
  `Retransmit*`、`Observe*`、`Block1`/`Block2`、分块结果）定义在该包，根包以类型别名重新导出，
  已有的 import、名称与接口断言全部照旧。`gcoap/hook` 不 import 根包。

### 新增

- `fault_matrix_test.go`（连同 `fault_test.go` 与 `fault_client_test.go`）：弱网故障注入
  矩阵。每个「故障 × 特性」一个子测试 —— 重复、丢包、乱序、迟到、重置、重连，分别打在
  请求、separate response、observe 通知、Block1 上传与客户端侧。所有场景跑在手动时钟
  （`fakeClock`）加真实回环 UDP 上，断言 handler 次数、逐字节一致的重传与最终状态，全程
  不 sleep；表里缺哪条「故障 × 特性」一眼可见。

- `Config.MaxObserversPerPeer`（32）与 `Config.MaxBlockUploadsPerPeer`（16）限制单个对端
  能占的 observer 与 Block1 上传槽位，一个对端不再能吃满全局表（`MaxObservers` 256、
  `MaxBlockUploads` 128）。超过 per-peer 上限的注册或上传回 `5.03 Service Unavailable`，
  该对端已有的观察与上传继续工作，其他对端不受影响；观察、上传或会话结束时释放配额。
  0 取默认值，per-peer 上限大于等于全局上限时不再额外生效。对端身份就是去重使用的传输
  层对端身份，所以重连从头计。

- `benchmark_test.go` 与 `docs/PERFORMANCE.md`（含中文版）：回环 UDP 上一次 confirmable
  往返、一次 Block2 下载、一条 non-confirmable/confirmable observe 通知的代价，附机器、边界
  与可直接照抄的复现命令。按协议库基线，基准不进 `go test ./...`、不进 CI，数字是基线而非
  目标。

- 两个可运行示例，每个都是自包含的 `main`，各自在回环端口上起服务端：
  `examples/server-client` 演示一次 confirmable 往返、没人注册的路径得到的 4.04，以及可被
  外部 CoAP 客户端查询的 `-serve` 模式；`examples/observe-client` 注册观察、打印随后到达的
  通知再注销。两者都打印 `[n/N]` 步骤与 `[done]`，Ctrl-C 停止且退出码 0，`-h` 给出可直接
  复制的命令。每个示例回答什么问题见 `examples/README.md` 与其中文版。

- `docs/ARCHITECTURE.md` 与 `docs/ARCHITECTURE.zh-CN.md`：包地图、服务端与客户端的 goroutine
  和锁归属、交换/观察/块传输状态机、限额表与指标契约。

- `FuzzDecodeMessage`：对端发来的每个数据报都要过解码器，所以这个 fuzz 目标钉住三条性质
  —— 不 panic、payload 与选项不超出输入规模、`encode(decode(x))` 稳定 —— 种子覆盖
  CON/NON/ACK/RST、Observe、Block1 与 Block2。

- CI 补上库缺的两道闸：Linux 上的 `race`，以及 `FuzzDecodeMessage` 的 10 秒 `fuzz`。
  `test` 矩阵保持三个 OS 的 `test`/`vet` 与互操作模块。

- `hook.MetricsFuncs`：用可选函数字段实现五个指标方法，nil 字段直接跳过。

- Block1 上传状态有界：`Config.MaxBlockUploads`（128）与 `Config.MaxBlockPayload`（1 MiB，同时校验声明的 Size1）限制单个异常客户端能占用的状态与内存，`Config.BlockIdleTimeout`（60s）回收空闲上传与完成响应。改变 method/URI/块大小或乱序的块回 4.08；超限回 4.13；超过并发上限的新上传回 5.03。重复块重放 2.31 且不重复追加；带新 message id 的重复最后一块重放缓存的最终响应。
- 大于 `Config.BlockSize`（1024）的 GET 响应即使请求未带 Block2 选项也会切片，使超过 MTU 的响应在受限链路上可送达。
- `Client.Do` 并发安全：去掉全局串行锁，分配 message id 时避让在途交换，每个调用方保有独立 token。`ClientConfig.MaxResponseSize`（1 MiB）限制 Block2 组装，超出返回 `ErrResponseTooLarge`。
- `Config.Metrics` 与 `ClientConfig.Metrics` 接收低基数事件（`Exchange`、`Dedup`、`Retransmit`、`Observe`、`Block`）；nil 零成本，`NoopMetrics` 是显式空实现。
- `Config.MaxObservers`（256）限制 observer 注册，超出回 5.03。
- 会话关闭时会清理其上传状态（旧 helper 从未被调用）。

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
