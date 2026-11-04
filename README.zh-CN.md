[English](README.md) | 简体中文

# gcoap

极简的 Go **CoAP 服务端状态机**，构建在
[gnety](https://github.com/maxid/gnety) 网络框架之上。

> 状态：**开发快照（未打标签）** — 请求路由、
> confirmable/non-confirmable 响应、Observe/Notify 以及链路双方的
> 块传输。DTLS 可用。
> 在稳定版本发布前不会打标签；请使用 commit 伪版本（pseudo-version）。

## 特性

- 经 `gnety/codec/coap` 提供 CoAP 线格式（不重复实现数据包编解码器）
- 经 gnety 提供 UDP 传输
- 按方法 + URI 路径进行请求路由
- CON → ACK 捎带响应，NON → NON 响应
- 查询解析与 URI-path 选项
- Observe/Notify（RFC 7641）：注册、通知、取消，以及可选的
  confirmable 通知（含重传、ACK/RST 处理与每个观察至多一条待发通知）
- 块传输（RFC 7959）：服务端的 Block2 读取与 Block1 写入，
  客户端的 Block1 请求体上传与 Block2 响应组装
- 客户端状态机：`Dial` + `Do`（CON 重传、自动 Block1
  上传与 Block2 组装、可配置的块大小与消息体上限）
- 客户端 Observe（RFC 7641）：`Observe` 完成注册并返回注册
  响应，通知通过回调送达，遵循
  第 3.4 节的序号规则（过时的通知会被丢弃，confirmable 通知会被 ACK），
  `Observation.Cancel` 或 context 取消即注销
- 分离响应（RFC 7252 第 5.2 节）：耗时超过 `ACK_TIMEOUT`
  的处理器会先收到一个空 ACK，随后其响应作为新的 CON 消息发送并
  重传；`Request.RespondLater` 让处理器显式选择这种方式
- RFC 7252 交换层：按对等方进行 message-id 去重（有界缓存、
  交换生命周期、对挂起的分离响应重放空 ACK）、
  带 `ACK_RANDOM_FACTOR` 抖动的指数退避重传与
  `MAX_RETRANSMIT`、可取消的处理器 context
- 经 gnety 提供 DTLS 传输（服务端/客户端）
- 未知路由返回 4.04

## 示例

```go
srv, _ := gcoap.New(gcoap.Config{Addr: ":5683"})
srv.Handle(coap.CodeGET, "/hello", func(ctx context.Context, req gcoap.Request) gcoap.Response {
    return gcoap.Response{Code: coap.CodeContent, Payload: []byte("hello")}
})
_ = srv.Start(context.Background())
```

客户端请求与观察：

```go
cli, _ := gcoap.Dial(context.Background(), "127.0.0.1:5683")
defer cli.Close()

// A body larger than ClientConfig.BlockSize is uploaded with Block1.
resp, err := cli.Do(ctx, gcoap.ClientRequest{
    Code: coap.CodePUT, Path: "/config", Payload: body,
})

obs, initial, err := cli.Observe(ctx, gcoap.ClientRequest{
    Code: coap.CodeGET, Path: "/state",
}, func(resp gcoap.ClientResponse) {
    fmt.Println(string(resp.Payload)) // later notifications
})
defer obs.Cancel(context.Background())
fmt.Println(string(initial.Payload)) // registration response
```

## 可运行示例

[`examples/`](examples/README.md) 下有两个示例，都不需要外部服务器：
[`server-client`](examples/server-client/main.go) 做一次 confirmable 请求/响应往返，
并展示没人注册的路径会被答成 4.04；[`observe-client`](examples/observe-client/main.go)
注册一个 CoAP 观察，打印随后到达的通知。两者都自己在回环地址上起服务端并打印实际地址：

```sh
go run ./examples/server-client
go run ./examples/observe-client
```

每个示例回答什么问题、怎么跑、应该看到什么，见
[`examples/README.md`](examples/README.zh-CN.md)；加 `-serve` 可以把任一示例变成一个
外部 CoAP 客户端可以直接查询的服务端。

## 性能

一次往返、一次 Block2 下载与一条 observe 通知的实测数字（含机器、边界与复现命令）见
[`docs/PERFORMANCE.zh-CN.md`](docs/PERFORMANCE.zh-CN.md)。它是回环基线，不是吞吐承诺。

## 限额与指标

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `Config.BlockSize` | 1024 | 大响应切片使用的 Block2 大小 |
| `Config.MaxObservers` | 256 | 全局 observer 上限，超出注册回 5.03 |
| `Config.MaxObserversPerPeer` | 32 | 单个对端可持有的 observer 数，超出该对端注册回 5.03 |
| `Config.MaxBlockUploads` | 128 | 进行中 Block1 上传上限，超出新上传回 5.03 |
| `Config.MaxBlockUploadsPerPeer` | 16 | 单个对端进行中的 Block1 上传数，超出该对端新上传回 5.03 |
| `Config.MaxBlockPayload` | 1 MiB | 单个上传总字节，也用于 Size1 预检（4.13） |
| `Config.BlockIdleTimeout` | 60s | 空闲上传与完成响应的清理 |
| `ClientConfig.MaxResponseSize` | 1 MiB | Block2 组装上限，超出 `ErrResponseTooLarge` |

`Config.Metrics` 与 `ClientConfig.Metrics` 接收低基数事件：`Exchange(method, result, duration)`、
`Dedup()`、`Retransmit(kind)`、`Observe(event)`、`Block(direction, result)`。nil 钩子零成本，
`NoopMetrics` 是显式空实现；标签不携带 token、URI path 或对端地址。

## 扩展点

可观测性契约在 [`hook`](hook/)：`Metrics` 与它上报的粗粒度标签（`Result*`、`Retransmit*`、
`Observe*`、`Block1`/`Block2` 与分块结果）。根包把它们全部以类型别名重新导出，因此两条 import
路径是同一种类型，已有代码不用改：

```go
var m gcoap.Metrics = hook.MetricsFuncs{} // 同一个类型
```

`hook.MetricsFuncs` 用可选函数字段实现这五个方法——只设你关心的事件，nil 字段直接跳过；
`hook.NoopMetrics` 全部丢弃。

Handler 仍留在根包（`Handler` 就是带服务端 `Request`/`Response` 的普通 func 类型），`Server`
自己暴露路由面：这两者才是本包的主扩展点，metrics 钩子只做观测。

要让扩展在运行期可开关、可观测，把服务端套进插件注册表：
[`gplugin`](https://github.com/maxid/gplugin) 的 `gplugin.NewRegistry(server)` 提供具名插件、
三态生命周期、观察者与单品启停；本包只定义"插件能插到哪里"。

## 互操作测试

针对独立 CoAP 实现（`github.com/plgd-dev/go-coap/v3`）的测试放在独立的
[`interop`](interop/README.zh-CN.md) 模块里，因此本模块的依赖图不携带第三方 CoAP 栈：

```sh
cd interop && go test ./...
```

覆盖 UDP 与 DTLS 双向，包括 Block1、Block2 与 Observe。

## 路线图

- 服务端有界的 Block1 上传状态（TTL、全局上限）— 已完成
- 独立的互操作性测试与弱网测试 — 互操作已完成，弱网测试待做
- 服务端 per-peer 的 observer/upload 配额

## 许可证

MIT。见 [LICENSE](LICENSE)。
