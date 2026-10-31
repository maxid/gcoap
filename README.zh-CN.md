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

[`examples/server-client`](examples/server-client/main.go) 在同一程序内运行服务端与客户端：
服务端暴露 `/hello`，客户端发送 confirmable GET 并打印响应，然后双方停止。
服务端监听回环地址的临时端口并打印实际地址：

```sh
go run ./examples/server-client
go run ./examples/server-client -addr 127.0.0.1:5683 -path /hello
```

## 路线图

- 服务端有界的 Block1 上传状态（TTL、全局上限）
- 独立的互操作性测试与弱网测试

## 许可证

MIT。见 [LICENSE](LICENSE)。
