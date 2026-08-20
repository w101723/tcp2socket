# tcp2socket

[![Release](https://github.com/w101723/tcp2socket/actions/workflows/release.yml/badge.svg)](https://github.com/w101723/tcp2socket/actions/workflows/release.yml)

一个零第三方依赖的 Go 网络转发工具。`tcp2socket` 可在 TCP、Unix Socket 与标准输入/输出之间建立双向字节流转发，也可在 SSH `exec` 场景中通过 `socks-stdio` 提供一个轻量 SOCKS5 服务端。

## 特性

- TCP ↔ TCP、TCP ↔ Unix Socket、Unix Socket ↔ Unix Socket 双向转发
- `connect` 模式：以 stdin/stdout 转发 TCP 或 Unix Socket，可替代许多 `nc host port` 用法
- `socks-stdio` 模式：通过 stdin/stdout 提供 SOCKS5 `CONNECT` 服务，不依赖额外 SOCKS 守护进程
- 支持 TCP 半关闭，适用于需要请求完成后继续读取响应的协议
- 可配置目标拨号超时和代理连接空闲超时
- Unix Socket 自动清理旧 socket，并拒绝删除普通文件
- 单一静态二进制、无第三方依赖

## 安装

### 从 Release 下载

每次推送到 `main` 都会创建一个以 `commit-<完整 commit hash>` 标识的 Release，并提供：

| 平台 | 文件名 |
| --- | --- |
| macOS Intel | `tcp2socket-darwin-amd64` |
| macOS Apple Silicon | `tcp2socket-darwin-arm64` |
| Linux x86_64 | `tcp2socket-linux-amd64` |
| Linux ARM64 | `tcp2socket-linux-arm64` |

下载后赋予可执行权限：

```bash
chmod +x tcp2socket-linux-amd64
./tcp2socket-linux-amd64 -h
```

### 从源码构建

要求 Go 1.22 或更高版本：

```bash
git clone https://github.com/w101723/tcp2socket.git
cd tcp2socket
go build -o tcp2socket .
```

交叉编译示例：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags="-s -w" -o tcp2socket-linux-amd64 .
```

## 快速开始

### 将本地 TCP 端口转发至目标 TCP 服务

```bash
./tcp2socket -l :8000 -t 127.0.0.1:8999
```

此后访问本机 `:8000` 的连接会被双向转发到 `127.0.0.1:8999`。

### 使用 Unix Socket 作为目标

```bash
./tcp2socket -l :8000 -t unix:///tmp/backend.sock
```

### 作为 `nc` 替代品

```bash
./tcp2socket 127.0.0.1:8999
# 等同于：./tcp2socket connect tcp://127.0.0.1:8999
```

stdin 会发送到目标服务，目标响应会写到 stdout。因此日志始终写到 stderr，不会破坏传输数据。

## 使用方式

### 1. 通用代理模式

```text
tcp2socket -l <listen> -t <target> [options]
```

支持任意 TCP/Unix Socket 组合：

```bash
# TCP → TCP
./tcp2socket -l :8000 -t 192.168.1.10:1080

# TCP → Unix Socket
./tcp2socket -l :8000 -t unix:///tmp/backend.sock

# Unix Socket → TCP
./tcp2socket -l unix:///tmp/local.sock -t 127.0.0.1:8999

# Unix Socket → Unix Socket
./tcp2socket -l unix:///tmp/in.sock -t unix:///tmp/out.sock
```

常用选项：

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `-l`, `-listen` | — | 监听端点，必填 |
| `-t`, `-target` | — | 目标端点，必填 |
| `-dial-timeout` | `10s` | 目标连接建立超时 |
| `-idle-timeout` | `0` | 连接空闲超时；`0` 表示关闭 |
| `-unix-mode` | `0660` | 监听 Unix Socket 的权限 |
| `-v` | `false` | 输出连接日志到 stderr |

### 2. `connect`：STDIO ↔ Socket

```text
tcp2socket connect [options] <target>
tcp2socket <target>
```

```bash
# STDIO → TCP
./tcp2socket connect tcp://127.0.0.1:8999

# STDIO → Unix Socket
./tcp2socket connect unix:///tmp/app.sock

# 使用选项时，将选项放在 target 前
./tcp2socket connect -dial-timeout 5s -v tcp://127.0.0.1:8999
```

数据流：

```text
stdin  → target socket
target socket → stdout
```

### 3. `socks-stdio`：SOCKS5 over STDIO

```text
tcp2socket socks-stdio [options]
```

该模式在 stdin/stdout 上提供一个 SOCKS5 服务端。它会读取 SOCKS5 请求，直接连接请求中的目标 TCP 地址，然后双向转发数据。

```bash
./tcp2socket socks-stdio
./tcp2socket socks-stdio -dial-timeout 10s -v
```

当前协议范围：

- SOCKS5
- 无认证（`NO AUTHENTICATION REQUIRED`）
- `CONNECT` 命令
- IPv4、IPv6 与域名地址

不支持 SOCKS4、用户名/密码认证、`BIND` 或 UDP ASSOCIATE。每个进程处理一个 SOCKS5 会话；该模式不在单条 stdio 通道上进行多路复用。

## SSH 场景

### 透传至远端已有服务

将远端 `nc` 替换为 `tcp2socket`：

```bash
# 原方式
ssh -T host 'exec nc 127.0.0.1 8999'

# 使用 tcp2socket
ssh -T host 'exec tcp2socket 127.0.0.1:8999'
```

### 远端直接提供 SOCKS5

无需在远端预先启动 SOCKS 服务：

```bash
ssh -T host 'exec tcp2socket socks-stdio'
```

`connect` 和 `socks-stdio` 的 stdout 都是协议/业务数据通道。请勿包装会向 stdout 输出文本的命令；诊断日志会写到 stderr。

## 端点格式

| 格式 | 示例 | 含义 |
| --- | --- | --- |
| TCP URI | `tcp://127.0.0.1:8080` | TCP 端点 |
| Unix URI | `unix:///tmp/app.sock` | 绝对路径 Unix Socket |
| TCP 简写 | `:8080`、`127.0.0.1:8080` | TCP 端点 |
| Unix 简写 | `/tmp/app.sock` | Unix Socket |

`unix://` 格式必须使用绝对路径。

## 安全与运行建议

- `socks-stdio` 无认证。请仅通过受信任的 SSH 会话、受控 stdin/stdout 或其他受限传输方式使用它。
- 不要将无认证 SOCKS 服务直接暴露到不可信网络。
- 监听 Unix Socket 时，工具只会移除已有的 socket；若目标路径是普通文件，程序会拒绝删除。
- 在生产环境中，请按实际需要设置 `-dial-timeout`、`-idle-timeout` 与 `-unix-mode`。

## 开发与验证

```bash
gofmt -w .
go test ./...
go vet ./...
```

## License

当前仓库尚未包含许可证文件。若计划分发或接受外部贡献，建议在发布前添加明确的开源许可证。
