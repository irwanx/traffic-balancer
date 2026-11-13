<div align="center">

# traffic-balancer

**Tiny, fast HTTP load balancer in Rust, with health checks, failover and WebSocket support.**

[![License: MIT](https://img.shields.io/github/license/irwanx/traffic-balancer?style=flat-square&color=blue)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-2021-orange?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![tokio](https://img.shields.io/badge/tokio-1.x-informational?style=flat-square)](https://tokio.rs/)
[![hyper](https://img.shields.io/badge/hyper-1.x-informational?style=flat-square)](https://hyper.rs/)
[![WebSocket](https://img.shields.io/badge/WebSocket-supported-brightgreen?style=flat-square)](#websocket-and-socketio)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20windows%20%7C%20macos-lightgrey?style=flat-square)](#build-and-deploy)

[![Stars](https://img.shields.io/github/stars/irwanx/traffic-balancer?style=flat-square&logo=github)](https://github.com/irwanx/traffic-balancer/stargazers)
[![Forks](https://img.shields.io/github/forks/irwanx/traffic-balancer?style=flat-square&logo=github)](https://github.com/irwanx/traffic-balancer/network/members)
[![Issues](https://img.shields.io/github/issues/irwanx/traffic-balancer?style=flat-square)](https://github.com/irwanx/traffic-balancer/issues)
[![Last commit](https://img.shields.io/github/last-commit/irwanx/traffic-balancer?style=flat-square)](https://github.com/irwanx/traffic-balancer/commits/main)
[![Repo size](https://img.shields.io/github/repo-size/irwanx/traffic-balancer?style=flat-square)](https://github.com/irwanx/traffic-balancer)

[Features](#features) · [Quick start](#quick-start) · [Configuration](#configuration) · [How it works](#how-it-works) · [Deploy](#build-and-deploy) · [Limitations](#limitations)

</div>

---

A small, dependency-light HTTP/1.1 reverse proxy and load balancer written in Rust
(tokio + hyper). It spreads requests across several backend instances, takes dead
backends out of rotation automatically, and keeps WebSocket / Socket.IO traffic on a
single backend so stateful sockets keep working.

Everything lives in one file: [`src/main.rs`](src/main.rs).

## Features

- **Round-robin** across healthy backends.
- **Health checks**: active (`GET /health` every 30 s) and passive (a failed proxied
  request marks the backend down, a successful one marks it up again).
- **Automatic retry** on another backend when one fails (see [Retry rules](#retry-rules)).
- **WebSocket proxying** (`Upgrade: websocket`), so Socket.IO's websocket transport works.
- **Socket pinning**: `/socket.io/*` and `/docs/chat*` always go to the same backend, with
  failover to the next healthy one.
- **Built-in endpoints**: `/health` and `/__status` answered by the balancer itself.
- Single static binary, no config file.

## Quick start

```bash
git clone https://github.com/irwanx/traffic-balancer.git
cd traffic-balancer
cargo run --release
```

By default it listens on `127.0.0.1:8888` and balances between `127.0.0.1:8080` and
`127.0.0.1:8082`.

Point it at your own backends with the `BACKENDS` environment variable
(comma-separated `host:port`, plain `http` only):

```bash
# Linux / macOS
BACKENDS=127.0.0.1:3001,127.0.0.1:3002,10.0.0.5:3000 cargo run --release
```

```powershell
# Windows PowerShell
$env:BACKENDS = "127.0.0.1:3001,127.0.0.1:3002"; cargo run --release
```

On startup it runs one health check and prints how many backends are up:

```
Load balancer berjalan di http://127.0.0.1:8888 -> ["127.0.0.1:8080", "127.0.0.1:8082"] (2 hidup)
```

## Configuration

Most settings are constants at the top of `src/main.rs`; change them and rebuild.

| Setting | Default | Where | Meaning |
|---|---|---|---|
| `LISTEN_ADDR` | `127.0.0.1:8888` | const | Address to listen on. Use `0.0.0.0:PORT` to accept outside connections. |
| `BACKENDS` | `127.0.0.1:8080,127.0.0.1:8082` | env (falls back to `DEFAULT_BACKENDS`) | Backend list, `host:port`, no scheme. `https://` is rejected at startup. |
| `HEALTH_PATH` | `/health` | const | Path probed on each backend. |
| `CHECK_INTERVAL` | 30 s | const | Active health check interval. |
| `HEALTH_TIMEOUT` | 3 s | const | Timeout for one health probe. |
| `CONNECT_TIMEOUT` | 3 s | const | Timeout to connect to a backend. |
| `REQUEST_TIMEOUT` | 30 s | const | Time to wait for the backend's response headers. |
| `MAX_BODY` | 10 MB | const | Max request body (larger gets `413`). |
| `SOCKET_PIN` | `0` | const | Index of the backend that socket traffic prefers. |

## How it works

### Load balancing and health

Requests are distributed round-robin over backends currently marked healthy. A backend
counts as healthy when its `/health` answers with any status below 500, so a `404` still
means "alive". If every backend is marked down, the balancer tries all of them anyway so
recovery does not have to wait for the next 30 s check.

Backend state changes are logged:

```
[health] backend 127.0.0.1:8080 MATI          (marked down)
[health] backend 127.0.0.1:8080 HIDUP kembali (back up)
```

### Retry rules

The request body is buffered (up to `MAX_BODY`) so it can be replayed on another backend.

- **Connection refused**: retried on the next backend for every HTTP method (the request
  never reached the server).
- **Failure or timeout mid-request**: retried only for `GET`, `HEAD` and `OPTIONS`.
  Other methods such as `POST` get `502` / `504` and are not retried, to avoid running a
  non-idempotent request twice.
- If no backend can be reached: `503`.

### WebSocket and Socket.IO

Requests with `Upgrade: websocket` are proxied as raw upgraded connections: the handshake
is forwarded to the backend and, after `101 Switching Protocols`, bytes are copied in both
directions.

Socket.IO also starts with HTTP long-polling before upgrading, and all of those requests
must reach the same server. So any path starting with `/socket.io/` or `/docs/chat`, plus
every WebSocket upgrade, is **pinned** to one backend (`SOCKET_PIN`, the first in the list).
If that backend is down, traffic fails over to the next healthy one. Everything else stays
round-robin.

This suits apps whose socket state lives in process memory (online counters, in-memory
rooms). If you later share state between instances (for example with the Socket.IO Redis
adapter), you can remove the pinning and balance sockets like any other traffic.

To pin different paths, edit `is_sticky_path()` in `src/main.rs`.

### Built-in endpoints

| Endpoint | Response |
|---|---|
| `GET /health` | `200 ok` if at least one backend is healthy, otherwise `503`. |
| `GET /__status` | Plain text, one line per backend: `127.0.0.1:8080 up`. |

These are answered by the balancer and never forwarded, so a backend cannot expose its own
`/health` or `/__status` through it.

## Build and deploy

```bash
cargo build --release
# binary: target/release/traffic-balancer
```

### Cross-compile for a Linux server from Windows

The project is pure Rust (no OpenSSL), so a static musl binary builds cleanly with
[`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild):

```powershell
rustup target add x86_64-unknown-linux-musl
pip install ziglang
cargo install cargo-zigbuild
cargo zigbuild --release --target x86_64-unknown-linux-musl
```

The binary is at `target/x86_64-unknown-linux-musl/release/traffic-balancer`. Use
`aarch64-unknown-linux-musl` for ARM servers. Upload it with `scp` (send it under a
temporary name and `mv` it into place to avoid "Text file busy").

### Run as a service

systemd:

```ini
[Unit]
Description=traffic-balancer
After=network.target

[Service]
ExecStart=/opt/traffic-balancer/traffic-balancer
Environment=BACKENDS=127.0.0.1:8080,127.0.0.1:8082
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

or pm2:

```bash
pm2 start /opt/traffic-balancer/traffic-balancer --name traffic-balancer
pm2 save
```

## Limitations

- **Plain HTTP/1.1 to backends only.** No TLS to backends, and no TLS termination on the
  listener. Put it behind nginx, Caddy or a tunnel for HTTPS.
- **Forwarding headers are not added.** There is no `X-Forwarded-For` / `X-Forwarded-Proto`,
  and the original `Host` header is passed through. Backends will see the balancer as the
  client IP (`127.0.0.1`), which matters for IP-based rate limiting or banning. Add the
  headers in `handle()` if you need them.
- **Request bodies are buffered in memory** (max 10 MB), so large uploads and streaming
  request bodies are not supported. Response bodies are streamed.
- Hop-by-hop headers are stripped from proxied requests, but not from responses.
- Round-robin only. No weights, least-connections or per-route rules.
- Backend list is fixed at startup (no hot reload).
- Log messages are in Indonesian.

## Troubleshooting

- **`hyper::Error(IncompleteMessage)` in the log**: the client closed the connection before
  the request or response finished (page navigation, player seek, mobile network drop,
  upstream proxy timeout). Harmless.
- **`400` on `/socket.io/`**: the request reached a backend that does not know the session.
  Make sure Socket.IO traffic goes through this balancer so it is pinned, and that your
  proxy in front forwards the `Upgrade` and `Connection` headers (nginx needs
  `proxy_http_version 1.1; proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection "upgrade";`).
- **Status codes like `503` for specific routes**: the balancer relays whatever the backend
  returns. Check the backend directly (`curl http://127.0.0.1:8080/...`).

## License

Released under the [MIT License](LICENSE). Copyright (c) 2026 Irwanx.
