[English](PERFORMANCE.md) | 简体中文

# 性能：一台机器，回环 UDP

> 2026-11-04 在下面这台机器上测得。这是**基线，不是承诺**：它说明当前代码在一台主机上的
> 代价，存在的意义是让后续改动有可比较的参照。这里的数字既不是吞吐目标，也没有被外推到
> 真实网络。

## 机器与工具链

| 项 | 值 |
|---|---|
| CPU | Apple M1 Max（10 核：8 性能核 + 2 能效核） |
| 系统 / 内核 | macOS（darwin/arm64） |
| Go | go1.27.1 |
| `GOMAXPROCS` | 默认（10）；下面并行那几行用 `-cpu` 显式设置 |
| 传输 | 回环 UDP（`127.0.0.1`），服务端与客户端在同一进程 |
| 提交 | `c632aa8` 加上 `benchmark_test.go` 里的基准 |

macOS 笔记本不是安静的机器：这些数字的轮次间波动大约 ±10 %，能效核还会让并行扩展看起来比
服务器 CPU 上更差。第三位有效数字请当噪声看。

## 测的是什么

`benchmark_test.go` 起一个真实的 gcoap 服务端和一个真实的 gcoap 客户端，走回环 UDP ——
不 mock 传输层 —— 只对稳态循环计时。建立监听、拨号、注册观察都在计时器之外。

| 基准 | 请求 | 3 次中位数 | 折算速率 |
|---|---|---|---|
| `BenchmarkRequest` | 一次 confirmable GET，64 字节响应，一次一个 | 46.0 µs/op | 21.7k 次往返/秒 |
| `BenchmarkRequestParallel`（`-cpu=4`） | 同上，所有 goroutine 并发 | 25.7 µs/op | 38.9k 次往返/秒 |
| `BenchmarkRequestParallel`（`-cpu=10`） | 同上，所有 goroutine 并发 | 29.2 µs/op | 34.2k 次往返/秒 |
| `BenchmarkRequestBlock2` | 一次 GET，8 KiB 响应，由 8 个 Block2 块组装 | 431 µs/op | 2.3k 请求/秒，19.0 MB/s |
| `BenchmarkObserveNotify/non-confirmable` | 一个 observer，一条 NON 通知，投递到回调 | 25.0 µs/op | 40.0k 通知/秒 |
| `BenchmarkObserveNotify/confirmable` | 一个 observer，一条 CON 通知，由客户端 ACK | 38.4 µs/op | 26.0k 通知/秒 |

每次操作的内存分配在所有轮次里一致，由同一批基准报告：

| 基准 | 字节/op | 分配次数/op |
|---|---|---|
| `BenchmarkRequest` | 68.6 kB | 64 |
| `BenchmarkRequestBlock2` | 594 kB | 600 |
| `BenchmarkObserveNotify/non-confirmable` | 66.7 kB | 26 |
| `BenchmarkObserveNotify/confirmable` | 67.4 kB | 47 |

## 怎么读这些数字

- **一次串行往返约 46 µs**，其中大约一半花在两次回环系统调用周围的交换簿记上，而不是线路上。
  所以单客户端的串行吞吐约 21k 请求/秒。
- **吞吐在四核附近就不再涨**（`-cpu=4` 时 38.9k 请求/秒，`-cpu=10` 时 34.2k）。这个上限是
  结构性的：一个 goroutine 读套接字，默认还在同一个 goroutine 上跑 handler（见
  `docs/ARCHITECTURE.zh-CN.md` 的「服务端：一个读循环，两种执行模式」）。客户端加再多
  goroutine 也追不过它。
- **块传输的代价是每块一次往返。** 8 KiB 分 8 块要 431 µs —— 约等于小请求延迟的九倍 ——
  19 MB/s 就是回环上 1 KiB 块大小的价格。更大的 `Config.BlockSize` 换更少的往返；实际受
  MTU 与受限链路约束。
- **confirmable 通知比 non-confirmable 贵约 13 µs**，因为它多一次 ACK 往返和一条自己的
  outbound 交换 goroutine。它还会自我限速：每个 observer 同一时刻最多一条在途通知，所以
  慢客户端拖慢的是流本身，而不是把队列撑大。
- **分配没有优化过。** 每次请求约 69 kB，主要来自 gnety 侧每个数据报的缓冲和 gcoap 侧每个
  交换的状态，没有任何池化。这些数字记录的是当前代价，不是目标。

## 复现

```sh
cd libs/gcoap

# 串行请求、Block2 与 observe 通知（3 次取中位数）：
go test -run '^$' -bench 'BenchmarkRequest$|BenchmarkRequestBlock2$|BenchmarkObserveNotify' -benchtime=2s -count=3

# 增量吞吐：1、4、10 个 P：
go test -run '^$' -bench 'BenchmarkRequestParallel' -benchtime=2s -count=3 -cpu=1,4,10
```

`-run '^$'` 把单元测试挡在外面；基准从不属于 `go test ./...`，也不进 CI —— 按协议库基线，
测数字是发布期的手工动作。

## 这些数字不覆盖什么

- **真实网络。** 没有丢包、没有 RTT、没有乱序、没有 NAT、没有小 MTU。重传、去重与
  separate response 路径由单元测试覆盖，不由这些基准覆盖。
- **DTLS。** 所有基准走明文 UDP；DTLS 握手与记录保护的开销没有测。
- **Block1 上传，以及带块传输体的观察。** 两者都没有基准。
- **服务端容量。** 一个客户端、一个 observer、一个进程。服务端能承载多少对端由
  `Config.MaxExchanges`、`MaxObservers`、`MaxBlockUploads` 决定，不由这些基准测量。
- **指标钩子。** 没有挂 `Metrics` 实现；会分配或阻塞的钩子会改变这些数字。
- **其它硬件。** ARM64 笔记本的数字不能迁移到小型 ARM32 网关 —— 而那恰恰是最需要这些数字的
  部署形态。
