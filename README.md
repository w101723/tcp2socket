# tcp2socket

一个零第三方依赖的 Go 网络转发工具，支持：

- TCP -> TCP
- TCP -> Unix Socket
- Unix Socket -> TCP
- Unix Socket -> Unix Socket
- STDIO -> TCP
- STDIO -> Unix Socket

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

每次代码推送到 `main` 分支时，GitHub Actions 会自动运行测试，并以完整 Git commit hash 创建或更新一个 GitHub Release。Release 中包含以下零 CGO 依赖的二进制文件：

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

## 3. SSH 场景

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

完整示例：

```bash
socat \
  TCP-LISTEN:8000,bind=0.0.0.0,reuseaddr,fork \
  SYSTEM:"ssh -T \
    -o 'ProxyCommand=ssh -i /Users/wangzq/data/evayinfo/code/env/ssh/dev -o IdentitiesOnly=yes -p 60022 -q sunrs@10.246.250.218 usm-ProxyCommand-nc --target %h:%p' \
    root@172.29.25.2 \
    'exec tcp2socket 127.0.0.1:8999'"
```

注意：connect 模式中 stdout 是数据通道，因此 tcp2socket 的日志全部输出到 stderr，不会污染传输数据。
