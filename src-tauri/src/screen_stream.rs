use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use jpeg_encoder::{ColorType, Encoder};
use screenshots::Screen;
use tokio::{
    net::TcpListener,
    spawn,
    sync::{broadcast, Mutex},
    task::{spawn_blocking, JoinHandle},
};
use tokio_tungstenite::accept_async;
use futures_util::{SinkExt, StreamExt};

struct StreamServerState {
    shutdown_tx: broadcast::Sender<bool>,
    capture_handle: JoinHandle<()>,
    ws_handle: JoinHandle<()>,
    ws_url: String,
}

static STREAM_SERVER: Mutex<Option<StreamServerState>> = Mutex::const_new(None);
static STREAM_RUNNING: AtomicBool = AtomicBool::new(false);
static FRAME_SENDER: Mutex<Option<broadcast::Sender<Vec<u8>>>> = Mutex::const_new(None);

#[derive(Clone)]
pub struct StreamConfig {
    pub fps: u32,
    pub quality: u8,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self { fps: 24, quality: 40 }
    }
}

pub async fn init_stream_server() {
    init_stream_with_config(StreamConfig::default()).await;
}

pub async fn init_stream_with_config(config: StreamConfig) {
    if STREAM_RUNNING.load(Ordering::Relaxed) {
        return;
    }

    let (frame_tx, _) = broadcast::channel::<Vec<u8>>(32);
    let (shutdown_tx, _) = broadcast::channel(1);

    {
        let mut frame_sender_guard = FRAME_SENDER.lock().await;
        *frame_sender_guard = Some(frame_tx.clone());
    }

    let capture_tx = frame_tx.clone();
    let shutdown_rx = shutdown_tx.subscribe();

    let capture_handle = spawn(async move {
        capture_loop(capture_tx, shutdown_rx, config).await;
    });

    let ws_shutdown_rx = shutdown_tx.subscribe();
    let ws_frame_tx = frame_tx.clone();
    
    let ws_handle = spawn(async move {
        ws_server_loop(ws_shutdown_rx, ws_frame_tx).await;
    });

    let ip = local_ip().unwrap_or_else(|| "127.0.0.1".into());
    let ws_url = format!("ws://{}:8766", ip);

    println!("{} WebSocket stream initialized at: {}", now_str(), ws_url);

    STREAM_RUNNING.store(true, Ordering::Relaxed);

    let mut guard = STREAM_SERVER.lock().await;
    *guard = Some(StreamServerState {
        shutdown_tx,
        capture_handle,
        ws_handle,
        ws_url,
    });
}

pub async fn start_stream_server(config: StreamConfig) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if STREAM_RUNNING.load(Ordering::Relaxed) {
        if let Some(url) = get_stream_url().await {
            return Ok(url);
        }
    }

    init_stream_with_config(config).await;
    
    if let Some(url) = get_stream_url().await {
        Ok(url)
    } else {
        Err("Failed to start stream server".into())
    }
}

pub fn is_stream_running() -> bool {
    STREAM_RUNNING.load(Ordering::Relaxed)
}

pub async fn get_stream_url() -> Option<String> {
    let guard = STREAM_SERVER.lock().await;
    guard.as_ref().map(|s| s.ws_url.clone())
}

pub async fn stop_stream_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut guard = STREAM_SERVER.lock().await;
    let state = guard.take();
    if state.is_none() {
        STREAM_RUNNING.store(false, Ordering::Relaxed);
        return Err("Stream server is not running".into());
    }

    let state = state.unwrap();
    let _ = state.shutdown_tx.send(true);

    tokio::time::timeout(Duration::from_secs(5), async {
        let _ = state.capture_handle.await;
        let _ = state.ws_handle.await;
    }).await.ok();

    STREAM_RUNNING.store(false, Ordering::Relaxed);

    {
        let mut frame_sender_guard = FRAME_SENDER.lock().await;
        *frame_sender_guard = None;
    }

    println!("{} Stream server stopped", now_str());
    Ok(())
}

async fn ws_server_loop(
    mut shutdown_rx: broadcast::Receiver<bool>,
    frame_tx: broadcast::Sender<Vec<u8>>,
) {
    let listener = match TcpListener::bind("0.0.0.0:8766").await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("{} Failed to bind WebSocket server: {:?}", now_str(), e);
            return;
        }
    };
    println!("{} WebSocket server listening on 0.0.0.0:8766", now_str());

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        println!("{} New connection from: {}", now_str(), addr);
                        let ws_frame_tx = frame_tx.clone();
                        spawn(async move {
                            if let Ok(ws_stream) = accept_async(stream).await {
                                handle_ws_client(ws_stream, ws_frame_tx).await;
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("{} Accept error: {:?}", now_str(), e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                println!("{} WebSocket server shutting down", now_str());
                break;
            }
        }
    }
}

async fn handle_ws_client(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    frame_tx: broadcast::Sender<Vec<u8>>,
) {
    let (mut sender, mut receiver) = ws_stream.split();
    let mut rx = frame_tx.subscribe();
    let mut frame_count = 0;

    println!("{} WebSocket client connected", now_str());

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(frame) => {
                        if frame.is_empty() {
                            continue;
                        }

                        frame_count += 1;
                        if frame_count % 30 == 0 {
                            println!("{} WS sent frame {} ({} bytes)", now_str(), frame_count, frame.len());
                        }

                        if let Err(e) = sender.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await {
                            eprintln!("{} WS send error: {:?}", now_str(), e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("{} WS rx.recv() error: {:?}", now_str(), e);
                        break;
                    }
                }
            }
            result = receiver.next() => {
                match result {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        eprintln!("{} WS recv error: {:?}", now_str(), e);
                        break;
                    }
                    None => {
                        println!("{} WebSocket client disconnected (total frames: {})", now_str(), frame_count);
                        break;
                    }
                }
            }
        }
    }
}

fn now_str() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let ms = now.as_millis();
    let sec = ms / 1000;
    let ms_part = ms % 1000;
    format!("[{:010}.{:03}]", sec, ms_part)
}

async fn capture_loop(
    tx: broadcast::Sender<Vec<u8>>,
    mut shutdown_rx: broadcast::Receiver<bool>,
    config: StreamConfig,
) {
    println!("{} Capture loop started (fps: {}, quality: {})", now_str(), config.fps, config.quality);

    let screen = match Screen::from_point(0, 0) {
        Ok(s) => {
            println!("{} Screen: {}x{}", now_str(), s.display_info.width, s.display_info.height);
            s
        }
        Err(e) => {
            eprintln!("{} Screen error: {:?}", now_str(), e);
            return;
        }
    };

    let interval_ms = 1000 / config.fps as u64;
    let capture_tx = tx.clone();
    let quality = config.quality;
    let screen_clone = screen.clone();

    spawn_blocking(move || {
        let mut capture_count = 0;
        let screen_width = screen_clone.display_info.width;
        let screen_height = screen_clone.display_info.height;
        let rgb_buf_size = (screen_width * screen_height * 3) as usize;
        let mut rgb_buf = Vec::with_capacity(rgb_buf_size);
        let mut jpeg_buf = Vec::with_capacity(1024 * 1024);
        
        loop {
            let start_time = std::time::Instant::now();
            
            match screen_clone.capture() {
                Ok(image) => {
                    let width = image.width();
                    let height = image.height();
                    let raw = image.into_vec();

                    rgb_buf.clear();
                    for rgba in raw.chunks(4) {
                        rgb_buf.push(rgba[0]);
                        rgb_buf.push(rgba[1]);
                        rgb_buf.push(rgba[2]);
                    }

                    jpeg_buf.clear();
                    let encoder = Encoder::new(&mut jpeg_buf, quality);

                    if let Err(e) = encoder.encode(&rgb_buf, width as u16, height as u16, ColorType::Rgb) {
                        eprintln!("{} JPEG encode error: {:?}", now_str(), e);
                        std::thread::sleep(Duration::from_millis(interval_ms));
                        continue;
                    }

                    let frame_size = jpeg_buf.len();
                    let elapsed = start_time.elapsed().as_millis();
                    
                    if capture_tx.send(jpeg_buf.clone()).is_err() {
                        eprintln!("{} Frame dropped - no subscribers", now_str());
                    } else {
                        capture_count += 1;
                        if capture_count % 30 == 0 {
                            println!("{} Frame {} ({}ms, {} bytes)", 
                                now_str(), capture_count, elapsed, frame_size);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} capture failed: {:?}", now_str(), e);
                }
            }

            if shutdown_rx.try_recv().is_ok() {
                println!("{} Capture loop shutting down (total: {})", now_str(), capture_count);
                break;
            }

            let elapsed = start_time.elapsed().as_millis() as u64;
            if elapsed < interval_ms {
                std::thread::sleep(Duration::from_millis(interval_ms - elapsed));
            }
        }
    });
}

pub fn local_ip() -> Option<String> {
    use std::net::UdpSocket;
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("8.8.8.8:80").ok()?;
    s.local_addr().ok().map(|a| a.ip().to_string())
}
