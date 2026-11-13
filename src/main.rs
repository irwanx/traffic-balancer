use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Empty, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderMap, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval_at, timeout, Instant};

const LISTEN_ADDR: &str = "127.0.0.1:8888"; // ganti 0.0.0.0 kalau mau diakses dari luar mesin
// Daftar backend bawaan (hardcode), format host:port. Bisa ditimpa lewat env BACKENDS (dipisah koma), mis.
// BACKENDS=127.0.0.1:8081,10.0.0.2:8080 ; kalau env tidak diisi, daftar di bawah yang dipakai.
const DEFAULT_BACKENDS: &[&str] = &[
    "127.0.0.1:8080",
    "127.0.0.1:8082",
];

const HEALTH_PATH: &str = "/health";
const CHECK_INTERVAL: Duration = Duration::from_secs(30);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30); // sampai header respons backend diterima
const MAX_BODY: usize = 10 * 1024 * 1024; // body request di-buffer supaya bisa di-retry

const HOP_HEADERS: [&str; 7] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

type Body = UnsyncBoxBody<Bytes, hyper::Error>;

struct Backend {
    addr: String,
    healthy: AtomicBool,
}

struct State {
    backends: Vec<Backend>,
    counter: AtomicUsize,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl State {
    fn set_health(&self, idx: usize, ok: bool) {
        let b = &self.backends[idx];
        if b.healthy.swap(ok, Ordering::Relaxed) != ok {
            println!(
                "[health] backend {} {}",
                b.addr,
                if ok { "HIDUP kembali" } else { "MATI" }
            );
        }
    }

    // Urutan percobaan: round-robin hanya di antara server sehat.
    // Kalau `sticky` diisi (socket.io / chat), backend dipilih konsisten dari hash klien
    // supaya handshake polling -> websocket selalu mendarat di server yang sama.
    // Kalau semua ditandai mati, coba semuanya sebagai upaya terakhir
    // supaya pemulihan tidak harus menunggu cek 30 detik berikutnya.
    fn pick_order(&self, sticky: Option<u64>) -> Vec<usize> {
        if let Some(k) = sticky {
            let n = self.backends.len();
            let pref = (k as usize) % n;
            let rotated: Vec<usize> = (0..n).map(|j| (pref + j) % n).collect();
            let healthy: Vec<usize> = rotated
                .iter()
                .copied()
                .filter(|&i| self.backends[i].healthy.load(Ordering::Relaxed))
                .collect();
            return if healthy.is_empty() { rotated } else { healthy };
        }
        let mut pool: Vec<usize> = (0..self.backends.len())
            .filter(|&i| self.backends[i].healthy.load(Ordering::Relaxed))
            .collect();
        if pool.is_empty() {
            pool = (0..self.backends.len()).collect();
        }
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % pool.len();
        (0..pool.len()).map(|k| pool[(start + k) % pool.len()]).collect()
    }

    fn healthy_count(&self) -> usize {
        self.backends
            .iter()
            .filter(|b| b.healthy.load(Ordering::Relaxed))
            .count()
    }
}

fn load_backends() -> Vec<String> {
    let raw = std::env::var("BACKENDS").unwrap_or_default();
    let raw = if raw.trim().is_empty() { DEFAULT_BACKENDS.join(",") } else { raw };
    let list: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().trim_start_matches("http://").trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if list.is_empty() {
        eprintln!("BACKENDS kosong");
        std::process::exit(1);
    }
    for a in &list {
        if a.starts_with("https://") {
            eprintln!("BACKENDS: '{}' memakai https, belum didukung (hanya http host:port)", a);
            std::process::exit(1);
        }
        let valid = format!("http://{}/", a)
            .parse::<Uri>()
            .map(|u| u.authority().is_some() && u.path() == "/")
            .unwrap_or(false);
        if !valid {
            eprintln!("BACKENDS: '{}' bukan format host:port yang valid", a);
            std::process::exit(1);
        }
    }
    list
}

fn text(status: StatusCode, msg: impl Into<String>) -> Response<Body> {
    let body = Full::new(Bytes::from(msg.into()))
        .map_err(|e| match e {})
        .boxed_unsync();
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(body)
        .unwrap()
}

async fn check_one(state: Arc<State>, idx: usize) {
    let uri: Uri = match format!("http://{}{}", state.backends[idx].addr, HEALTH_PATH).parse() {
        Ok(u) => u,
        Err(_) => return,
    };
    let req = Request::get(uri).body(Full::new(Bytes::new())).unwrap();
    // Hidup = server menjawab dengan status < 500 (404 berarti server hidup tapi belum punya /health)
    let ok = match timeout(HEALTH_TIMEOUT, state.client.request(req)).await {
        Ok(Ok(resp)) => !resp.status().is_server_error(),
        _ => false,
    };
    state.set_health(idx, ok);
}

async fn check_all(state: &Arc<State>) {
    let handles: Vec<_> = (0..state.backends.len())
        .map(|i| tokio::spawn(check_one(state.clone(), i)))
        .collect();
    for h in handles {
        let _ = h.await;
    }
}

async fn health_loop(state: Arc<State>) {
    let mut tick = interval_at(Instant::now() + CHECK_INTERVAL, CHECK_INTERVAL);
    loop {
        tick.tick().await;
        check_all(&state).await;
    }
}

fn is_sticky_path(path: &str) -> bool {
    path.starts_with("/socket.io/") || path.starts_with("/docs/chat")
}

fn is_ws_upgrade(h: &HeaderMap) -> bool {
    h.get(UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
}

// Semua trafik socket.io dan /docs/chat dikunci ke satu backend (index 0 dari daftar,
// geser otomatis ke backend hidup berikutnya kalau mati). Alasannya state socket ada
// di memori per instance (mis. _onlineUsers di server.js, room support chat).
const SOCKET_PIN: u64 = 0;

// Proxy WebSocket: teruskan handshake Upgrade ke backend, lalu salin byte dua arah.
async fn proxy_ws(mut req: Request<Incoming>, state: Arc<State>, key: u64) -> Response<Body> {
    let on_client = hyper::upgrade::on(&mut req);
    let (parts, _) = req.into_parts();
    let pq = parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/").to_string();
    let uri: Uri = match pq.parse() {
        Ok(u) => u,
        Err(_) => return text(StatusCode::BAD_REQUEST, "URI tidak valid"),
    };

    for idx in state.pick_order(Some(key)) {
        let addr = state.backends[idx].addr.clone();
        let stream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            _ => {
                state.set_health(idx, false);
                continue;
            }
        };
        let (mut sender, conn) = match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await {
            Ok(x) => x,
            Err(_) => {
                state.set_health(idx, false);
                continue;
            }
        };
        tokio::spawn(async move {
            let _ = conn.with_upgrades().await;
        });

        let mut out = Request::new(Empty::<Bytes>::new());
        *out.method_mut() = parts.method.clone();
        *out.uri_mut() = uri.clone();
        *out.headers_mut() = parts.headers.clone(); // header Upgrade/Connection/Sec-WebSocket-* dipertahankan

        let mut resp = match timeout(REQUEST_TIMEOUT, sender.send_request(out)).await {
            Ok(Ok(r)) => r,
            _ => {
                state.set_health(idx, false);
                eprintln!("[gagal-ws] {} -> {}", pq, addr);
                continue;
            }
        };
        println!("WS {} -> {} ({})", pq, addr, resp.status().as_u16());

        if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
            let on_backend = hyper::upgrade::on(&mut resp);
            tokio::spawn(async move {
                if let (Ok(c), Ok(b)) = (on_client.await, on_backend.await) {
                    let mut c = TokioIo::new(c);
                    let mut b = TokioIo::new(b);
                    let _ = tokio::io::copy_bidirectional(&mut c, &mut b).await;
                }
            });
            let (rparts, _) = resp.into_parts();
            let empty = Empty::<Bytes>::new().map_err(|e| match e {}).boxed_unsync();
            return Response::from_parts(rparts, empty);
        }
        return resp.map(|b| b.boxed_unsync());
    }
    text(StatusCode::SERVICE_UNAVAILABLE, "semua backend tidak bisa dihubungi")
}

async fn handle(req: Request<Incoming>, state: Arc<State>) -> Result<Response<Body>, Infallible> {
    // Endpoint milik balancer sendiri
    match req.uri().path() {
        "/health" => {
            return Ok(if state.healthy_count() > 0 {
                text(StatusCode::OK, "ok")
            } else {
                text(StatusCode::SERVICE_UNAVAILABLE, "tidak ada backend yang hidup")
            });
        }
        "/__status" => {
            let mut out = String::new();
            for b in &state.backends {
                let s = if b.healthy.load(Ordering::Relaxed) { "up" } else { "down" };
                out.push_str(&format!("{} {}\n", b.addr, s));
            }
            return Ok(text(StatusCode::OK, out));
        }
        _ => {}
    }

    let sticky_key = if is_sticky_path(req.uri().path()) || is_ws_upgrade(req.headers()) {
        Some(SOCKET_PIN)
    } else {
        None
    };
    if is_ws_upgrade(req.headers()) {
        return Ok(proxy_ws(req, state, SOCKET_PIN).await);
    }

    let (parts, body) = req.into_parts();
    let bytes = match Limited::new(body, MAX_BODY).collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            return Ok(if e.downcast_ref::<LengthLimitError>().is_some() {
                text(StatusCode::PAYLOAD_TOO_LARGE, "body request terlalu besar (maks 10 MB)")
            } else {
                text(StatusCode::BAD_REQUEST, "gagal membaca body request")
            });
        }
    };

    let mut headers = parts.headers.clone();
    for h in HOP_HEADERS {
        headers.remove(h);
    }
    let idempotent = matches!(parts.method, Method::GET | Method::HEAD | Method::OPTIONS);
    let pq = parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");

    for idx in state.pick_order(sticky_key) {
        let addr = state.backends[idx].addr.as_str();
        let uri: Uri = match format!("http://{}{}", addr, pq).parse() {
            Ok(u) => u,
            Err(_) => return Ok(text(StatusCode::BAD_REQUEST, "URI tidak valid")),
        };

        let mut out = Request::new(Full::new(bytes.clone()));
        *out.method_mut() = parts.method.clone();
        *out.uri_mut() = uri;
        *out.headers_mut() = headers.clone();

        match timeout(REQUEST_TIMEOUT, state.client.request(out)).await {
            Ok(Ok(resp)) => {
                state.set_health(idx, true);
                println!("{} {} -> {} ({})", parts.method, pq, addr, resp.status().as_u16());
                return Ok(resp.map(|b| b.boxed_unsync()));
            }
            Ok(Err(e)) => {
                state.set_health(idx, false);
                eprintln!("[gagal] {} {} -> {}: {}", parts.method, pq, addr, e);
                // Koneksi ditolak = request belum diproses, aman di-retry untuk semua method.
                if e.is_connect() || idempotent {
                    continue;
                }
                return Ok(text(
                    StatusCode::BAD_GATEWAY,
                    format!("backend {} gagal di tengah request (tidak di-retry)", addr),
                ));
            }
            Err(_) => {
                state.set_health(idx, false);
                eprintln!("[timeout] {} {} -> {}", parts.method, pq, addr);
                if idempotent {
                    continue;
                }
                return Ok(text(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("backend {} timeout (tidak di-retry)", addr),
                ));
            }
        }
    }

    Ok(text(StatusCode::SERVICE_UNAVAILABLE, "semua backend tidak bisa dihubungi"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(CONNECT_TIMEOUT));

    let state = Arc::new(State {
        backends: load_backends()
            .into_iter()
            .map(|addr| Backend { addr, healthy: AtomicBool::new(true) })
            .collect(),
        counter: AtomicUsize::new(0),
        client: Client::builder(TokioExecutor::new()).build(connector),
    });

    // Cek awal sebelum menerima trafik, lalu ulangi tiap CHECK_INTERVAL
    check_all(&state).await;
    tokio::spawn(health_loop(state.clone()));

    let listener = TcpListener::bind(LISTEN_ADDR).await?;
    println!(
        "Load balancer berjalan di http://{} -> {:?} ({} hidup)",
        LISTEN_ADDR,
        state.backends.iter().map(|b| b.addr.as_str()).collect::<Vec<_>>(),
        state.healthy_count()
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(req, state.clone()));
            if let Err(err) = http1::Builder::new().serve_connection(io, svc).with_upgrades().await {
                eprintln!("Error melayani koneksi: {:?}", err);
            }
        });
    }
}
