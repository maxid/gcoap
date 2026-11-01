[English](README.md) | 简体中文

# interop

针对独立 CoAP 实现（[`plgd-dev/go-coap/v3`](https://github.com/plgd-dev/go-coap)）的互操作测试。

测试放在独立的 Go module 里，因此本库的依赖图不携带第三方 CoAP 栈。该模块用 `replace`
把 `github.com/maxid/gcoap` 指向父目录，始终测试工作副本，只用于检出仓库后运行，不单独发布：

```sh
cd interop && go test ./...
```

覆盖：

- 我方 client ↔ 对方 server、对方 client ↔ 我方 server，UDP 与 DTLS 双向
- Block1 上传、Block2 下载、Observe（UDP）
- DTLS 方向使用自签证书

CI 单独运行该模块。父模块更新依赖 pin 后，在这里刷新校验和：

```sh
cd interop && go mod tidy
```
