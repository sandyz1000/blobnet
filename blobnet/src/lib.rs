//! A low-latency file server that responds to requests for chunks of file data.
//!
//! This acts as a non-volatile, over-the-network content cache. Internal users
//! can add binary blobs to the cache, and the data is indexed by its SHA-256
//! hash. Any blob can be retrieved by its hash and range of bytes to read.
//!
//! Data stored in this server is locally cached, backed by NFS, and durable.

use std::convert::Infallible;
use std::future::{self, Future};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Result};
use hyper::body::Bytes;
use hyper::client::HttpConnector;
pub use hyper::server::conn::AddrIncoming;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Request, Response, Server, StatusCode};
use named_retry::Retry;
use tokio::{fs, time};

use crate::handler::handle;

mod handler;
pub mod utils;

/// Configuration for the file server.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the local disk storage.
    pub storage_path: PathBuf,

    /// Path to the network file system mount.
    pub nfs_path: PathBuf,

    /// Secret used to authorize users to access the service.
    pub secret: String,
}

/// Create a file server listening at the given address.
pub async fn listen(config: Config, incoming: AddrIncoming) -> Result<(), hyper::Error> {
    listen_with_shutdown(config, incoming, future::pending()).await
}

/// Create a file server listening at the given address, with graceful shutdown.
pub async fn listen_with_shutdown(
    config: Config,
    incoming: AddrIncoming,
    shutdown: impl Future<Output = ()>,
) -> Result<(), hyper::Error> {
    let config = Arc::new(config);

    // Low-level service boilerplate to interface with the [`hyper`] API.
    let make_svc = make_service_fn(move |_conn| {
        let config = Arc::clone(&config);
        async {
            Ok::<_, Infallible>(service_fn(move |req| {
                let config = Arc::clone(&config);
                async {
                    let resp = handle(config, req).await;
                    Ok::<_, Infallible>(resp.unwrap_or_else(|code| {
                        Response::builder()
                            .status(code)
                            .body(Body::empty())
                            .unwrap()
                    }))
                }
            }))
        }
    });

    Server::builder(incoming)
        .tcp_nodelay(true)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown)
        .await
}

/// A background process that periodically cleans the cache directory.
///
/// Since the cache directory is limited in size but local to the machine, it is
/// acceptable to delete files from this folder at any time. Therefore, we can
/// simply remove 1/(256^2) of all files at an interval of 60 seconds.
///
/// Doing the math, it would take (256^2) / 60 / 24 = ~46 days on average to
/// expire any given file from the disk cache directory.
pub async fn cleaner(config: Config) {
    const CLEAN_INTERVAL: Duration = Duration::from_secs(30);
    loop {
        time::sleep(CLEAN_INTERVAL).await;
        let prefix = fastrand::u16(..);
        let (d1, d2) = (prefix / 256, prefix % 256);
        let subfolder = config.storage_path.join(&format!("{d1:x}/{d2:x}"));
        if fs::metadata(&subfolder).await.is_ok() {
            println!("cleaning cache directory: {}", subfolder.display());
            let subfolder_tmp = config.storage_path.join(&format!("{d1:x}/.tmp-{d2:x}"));
            fs::remove_dir_all(&subfolder_tmp).await.ok();
            if fs::rename(&subfolder, &subfolder_tmp).await.is_ok() {
                fs::remove_dir_all(&subfolder_tmp).await.ok();
            }
        }
    }
}

/// An asynchronous client for the file server.
#[derive(Clone)]
pub struct FileClient {
    client: Client<HttpConnector>,
    origin: String,
    secret: String,
    retry: Retry,
}

impl FileClient {
    /// Create a new file client object pointing at a given HTTP origin.
    pub fn new(origin: &str, secret: &str) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        FileClient {
            client: Client::builder().build(connector),
            origin: origin.into(),
            secret: secret.into(),
            retry: Retry::new("blobnet-file-client")
                .attempts(4)
                .base_delay(Duration::from_millis(50))
                .delay_factor(2.0),
        }
    }

    /// Send an HTTP request, retrying on server errors.
    ///
    /// This retry operation fixes rare transient disconnects of a few
    /// milliseconds when the blobnet server is terminated, due to a restart. It
    /// also retries if the body stream is canceled or closed abnormally.
    async fn request_with_retry<Fut>(
        &self,
        make_req: impl Fn() -> Fut,
    ) -> Result<(StatusCode, Bytes)>
    where
        Fut: Future<Output = Result<Request<Body>>>,
    {
        self.retry
            .run(|| async {
                let resp = self.client.request(make_req().await?).await?;
                let status = resp.status();
                ensure!(!status.is_server_error(), "server error: {status}");
                let bytes = hyper::body::to_bytes(resp.into_body()).await?;
                Ok((status, bytes))
            })
            .await
    }

    /// Check if a file is present in the server.
    pub async fn has(&self, hash: &str) -> Result<bool> {
        let make_req = || async {
            Ok(Request::builder()
                .method("GET")
                .uri(&format!("{}/{}", self.origin, hash))
                .header("X-Bn-Secret", &self.secret)
                .header("X-Bn-Range", "0-0")
                .body(Body::empty())?)
        };
        let (status, _) = self.request_with_retry(make_req).await?;
        match status {
            status if status.is_success() => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => bail!("has request failed: {status}"),
        }
    }

    /// Read a range of bytes from a file.
    pub async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let make_req = || async {
            let mut req = Request::builder()
                .method("GET")
                .uri(&format!("{}/{}", self.origin, hash))
                .header("X-Bn-Secret", &self.secret);
            if let Some((start, end)) = range {
                req = req.header("X-Bn-Range", format!("{}-{}", start, end));
            }
            Ok(req.body(Body::empty())?)
        };
        let (status, bytes) = self.request_with_retry(make_req).await?;
        ensure!(status.is_success(), "get request failed: {status}");
        Ok(bytes)
    }

    /// Put a stream of data to the server, returning the hash ID if successful.
    pub async fn put<Fut, B>(&self, data: impl Fn() -> Fut) -> Result<String>
    where
        Fut: Future<Output = Result<B>>,
        B: Into<Body>,
    {
        let make_req = || async {
            Ok(Request::builder()
                .method("PUT")
                .uri(&self.origin)
                .header("X-Bn-Secret", &self.secret)
                .body(data().await?.into())?)
        };
        let (status, bytes) = self.request_with_retry(make_req).await?;
        ensure!(status.is_success(), "put request failed: {status}");
        Ok(std::str::from_utf8(&bytes)?.into())
    }
}
