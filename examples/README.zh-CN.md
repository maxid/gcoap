[English](README.md) | 简体中文

# 示例

两个可直接运行的程序，每个回答一个问题。每个都是自包含的 `main` 包，在仓库根目录
`go run ./examples/<name>` 即可，不需要第三方 CoAP 服务器，也不需要配置文件。两者都
自己起一个回环地址上的 gcoap 服务端，所以可以并排跑，也不会占用真实的 CoAP 端口。

- [server-client](#server-client) —— confirmable 请求/响应一来一回长什么样、没人注册的
  路径会得到什么，以及怎么让外部 CoAP 客户端直接打 gcoap 服务端。
- [observe-client](#observe-client) —— CoAP 观察怎么工作：注册一次，之后不需要再发请求就
  持续收到通知。

## 统一约定

- 每个步骤一行 —— `[n/N] 动作 … 结果` —— 结束时一行 `[done] …`。
- 出错打到 stderr，格式 `<示例名>: <动作>: <错误>`，退出码非 0。
- Ctrl-C 都能停：脚本式的一轮打印 `[stop] interrupted`，长期运行的 `-serve` 打印
  `[stop] server stopped`，退出码都是 0 —— 示例不是服务。
- `-h` 打印这个示例回答什么问题，外加可以直接复制的命令。
- 脚本式的运行监听回环地址的临时端口并打印实际地址；`-serve` 用 CoAP 的
  `127.0.0.1:5683`，端口被占用时挪到下一个空闲 UDP 端口并打印出来，所以它打印的地址
  永远是可以直接用的那个。

## server-client

**回答：** 一次请求/响应要多少代码，以及没人注册的路径会被答成什么。程序起一个只挂一个
GET 资源的服务端，先对它发 confirmable GET，再 GET `/missing` 展示 4.04。

```sh
go run ./examples/server-client
go run ./examples/server-client -path /state -timeout 2s
go run ./examples/server-client -serve   # 然后：coap-client -m get coap://127.0.0.1:5683/hello
```

应该看到：

```
[1/4] server listening on 127.0.0.1:59983, resource GET /hello
[2/4] client dialed 127.0.0.1:59983
[3/4] CON GET /hello -> 2.05 "hello from gcoap for /hello"
[4/4] GET /missing -> 4.04 (every unknown route is 4.04)
[done] confirmable round trip complete; server and client closed
```

`-serve` 跳过客户端，只把服务端挂着 —— 这就是手动互操作模式：libcoap 的 `coap-client`、
另一个 gcoap 进程或任何别的 CoAP 实现都可以一直查这个资源，直到 Ctrl-C。

## observe-client

**回答：** 观察怎么注册、注册之后不发请求会收到什么、怎么取消。服务端暴露一个自己每
`-interval` 推进一次值的资源；客户端用带 `Observe=0` 的 GET 注册，打印注册响应和 `-count`
条通知，最后发 `Observe=1` 注销。

```sh
go run ./examples/observe-client
go run ./examples/observe-client -interval 100ms -count 5
go run ./examples/observe-client -serve   # 然后：coap-client -m get -s 30 coap://127.0.0.1:5683/sensor
```

应该看到：

```
[1/5] server listening on 127.0.0.1:51015, resource GET /sensor
[2/5] client dialed 127.0.0.1:51015
[3/5] registered: 2.05 "value=0" (the registration response is the current state)
[4/5] notification 1: 2.05 "value=1"
[4/5] notification 2: 2.05 "value=2"
[4/5] notification 3: 2.05 "value=3"
[5/5] deregistered after 3 notifications
[done] observation cancelled; server and client closed
```

输出里看不到的两个细节：

- RFC 7641 第 3.2 节规定注册响应就是第一条通知，所以 `Client.Observe` 把它作为返回值给
  你，而不再塞进回调。之后每条通知都带序号递增的 `Observe` 选项；`gcoap.Observation`
  会丢掉过期的通知，遇到 confirmable 的还会先 ACK。
- 回调跑在客户端的接收 goroutine 上，按到达顺序串行执行。它不能阻塞，所以这个示例只把
  通知投进带缓冲的 channel，打印放在别处。

观察功能两侧的状态机见 [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.zh-CN.md)；
observer 与通知的限额见 [README](../README.zh-CN.md)。
