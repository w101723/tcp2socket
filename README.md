# tcp2socket

一个零第三方依赖的 Go 网络转发工具，支持：

- TCP -> TCP
- TCP -> Unix Socket
- Unix Socket -> TCP
- Unix Socket -> Unix Socket
- STDIO -> TCP
- STDIO -> Unix Socket
- SOCKS5 -> STDIO（无认证，仅支持 TCP CONNECT）

其中 `connect` 模式可以直接替代很多 `nc host port` 场景，尤其适合 SSH 远端命令。

## 编译

```bash
go build -o tcp2socket .
```

Linux AMD64 静态编译：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o tcp2socket .
```

Linux ARM64：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
go build -trimpath -ldflags="-s -w" -o tcp2socket .
```

---

## 自动发布

每次代码推送到 `main` 分支时，GitHub Actions 会自动运行测试，并以 `commit-<完整 Git commit hash>` 创建或更新一个 GitHub Release。Release 中包含以下零 CGO 依赖的二进制文件：

- `tcp2socket-darwin-amd64`
- `tcp2socket-darwin-arm64`
- `tcp2socket-linux-amd64`
- `tcp2socket-linux-arm64`

---

## 1. 普通代理模式

```bash
tcp2socket -l <listen> -t <target>
```

### TCP -> TCP

```bash
./tcp2socket \
  -l :8000 \
  -t 127.0.0.1:8999
```

### TCP -> Unix

```bash
./tcp2socket \
  -l :8000 \
  -t /tmp/backend.sock
```

### Unix -> TCP

```bash
./tcp2socket \
  -l /tmp/local.sock \
  -t 127.0.0.1:8999
```

### Unix -> Unix

```bash
./tcp2socket \
  -l /tmp/in.sock \
  -t /tmp/out.sock
```

---

## 2. connect 模式

`connect` 模式把：

```text
stdin  -> socket
stdout <- socket
```

因此可以用于替代：

```bash
nc 127.0.0.1 8999
```

### STDIO -> TCP

```bash
./tcp2socket connect tcp://127.0.0.1:8999
```

或者直接：

```bash
./tcp2socket 127.0.0.1:8999
```

### STDIO -> Unix Socket

```bash
./tcp2socket connect unix:///tmp/app.sock
```

或者：

```bash
./tcp2socket /tmp/app.sock
```

---

## 3. SOCKS5 STDIO 模式

`socks-stdio` 在 stdin/stdout 上直接提供一个 SOCKS5 服务，不需要预先启动 `127.0.0.1:8999` 等独立 SOCKS 服务。每次命令执行处理一个 SOCKS5 会话，适合 `ssh` 远程执行场景。

当前仅支持：

- SOCKS5
- 无认证（`NO AUTHENTICATION REQUIRED`）
- `CONNECT` 命令
- IPv4、IPv6 和域名目标地址

不支持 SOCKS4、用户名/密码认证和 UDP ASSOCIATE。

```bash
./tcp2socket socks-stdio
```

可选参数：

```bash
./tcp2socket socks-stdio -dial-timeout 10s -v
```

SSH 示例：

```bash
ssh -T host 'exec tcp2socket socks-stdio'
```

其中 stdout 是 SOCKS5 协议与业务数据通道；日志始终写到 stderr，不能在该模式下向 stdout 输出其他内容。

---

## 4. SSH 场景

原命令：

```bash
ssh root@172.29.25.2 \
  'exec nc 127.0.0.1 8999'
```

替换为：

```bash
ssh root@172.29.25.2 \
  'exec tcp2socket 127.0.0.1:8999'
```

或者：

```bash
ssh root@172.29.25.2 \
  'exec tcp2socket connect tcp://127.0.0.1:8999'
```

或者直接使用内置 SOCKS5 服务：

```bash
ssh -T root@172.29.25.2 \
  'exec tcp2socket socks-stdio'
```

注意：connect 和 socks-stdio 模式中 stdout 都是数据通道，因此 tcp2socket 的日志全部输出到 stderr，不会污染传输数据。
