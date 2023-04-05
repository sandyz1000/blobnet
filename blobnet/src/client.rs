//! Basic HTTP client for the blobnet file server.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context};
use hyper::client::{connect::Connect, HttpConnector};
use hyper::{Body, Client, HeaderMap, Request, StatusCode};
use named_retry::Retry;

use crate::headers::{HEADER_FILE_LENGTH, HEADER_RANGE, HEADER_SECRET};
#[cfg(doc)]
use crate::provider::Remote;
use crate::utils::body_stream;
use crate::{BlobRange, BlobRead, Error};

/// hyper's client strictly maintains a single HTTP/2 connection
/// to a HOST:PORT destination, multiplexing new HTTP requests onto
/// this single connection.
///
/// As single flows, defined by a (source IP, source port, dest IP, dest port,
/// protocol) 5-tuple, are subject to lower bandwidth limits on cloud instances
/// (e.g. AWS EC2), we use a pool of clients to allow blobnet to saturate an
/// instance's available bandwidth.
#[derive(Clone)]
struct ClientPool<C> {
    // One HTTP/2 connection per-client, so number of connections equals number of clients.
    num_clients: u32,
    next_client: Arc<AtomicUsize>,
    clients: Vec<Client<C>>,
}

impl<C: Connect + Clone + Send + Sync + 'static> ClientPool<C> {
    pub fn new(connector: C, num_clients: u32) -> Self {
        assert!(num_clients >= 1, "number of clients must be at least 1");
        let clients: Vec<Client<C>> = (0..num_clients)
            .map(|_| {
                Client::builder()
                    .http2_max_frame_size(1 << 17)
                    .build(connector.clone())
            })
            .collect();
        ClientPool {
            num_clients,
            next_client: AtomicUsize::new(0).into(),
            clients,
        }
    }

    fn next_client(&self) -> &Client<C> {
        let next = match self.num_clients {
            1 => 0,
            // Wraps on overflow so safe to continually increment.
            _ => self.next_client.fetch_add(1, Ordering::SeqCst) % self.num_clients as usize,
        };
        &self.clients[next]
    }
}

/// An asynchronous client for the file server.
///
/// It is recommended to not use this client directly. Instead you should use
/// the [`Remote`] provider, which forwards requests to this client.
#[derive(Clone)]
pub struct FileClient<C> {
    client_pool: ClientPool<C>,
    origin: String,
    secret: String,
}

impl FileClient<HttpConnector> {
    /// Helper method that creates a client with an ordinary HTTP connector.
    pub fn new_http(origin: &str, secret: &str) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        Self::new(connector, origin, secret, 1)
    }

    /// Helper method that creates an HTTP client supporting multiple HTTP/2
    /// connections to the same HOST:PORT address.
    pub fn new_http_with_pool(origin: &str, secret: &str, num_connections: u32) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        Self::new(connector, origin, secret, num_connections)
    }
}

impl<C: Connect + Clone + Send + Sync + 'static> FileClient<C> {
    /// Create a new file client object pointing at a given origin.
    pub fn new(connector: C, origin: &str, secret: &str, num_clients: u32) -> Self {
        FileClient {
            client_pool: ClientPool::new(connector, num_clients),
            origin: origin.into(),
            secret: secret.into(),
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
    ) -> Result<(HeaderMap, Body), Error>
    where
        Fut: Future<Output = anyhow::Result<Request<Body>>>,
    {
        const HTTP_RETRY: Retry = Retry::new("blobnet-file-client")
            .attempts(4)
            .base_delay(Duration::from_millis(50))
            .delay_factor(2.0);

        let client = self.client_pool.next_client();
        let (status, headers, body) = HTTP_RETRY
            .run(|| async {
                let resp = client.request(make_req().await?).await?;
                let status = resp.status();

                ensure!(!status.is_server_error(), "server error: {status}");
                let headers = resp.headers().clone();
                let body = resp.into_body();
                Ok((status, headers, body))
            })
            .await?;

        match status {
            status if status.is_success() => Ok((headers, body)),
            StatusCode::NOT_FOUND => Err(Error::NotFound),
            StatusCode::RANGE_NOT_SATISFIABLE => Err(Error::BadRange),
            status => Err(anyhow!("blobnet request failed: {status}").into()),
        }
    }

    /// Check if a file is present in the server and return its size.
    pub async fn head(&self, hash: &str) -> Result<u64, Error> {
        let make_req = || async {
            Ok(Request::builder()
                .method("HEAD")
                .uri(&format!("{}/{}", self.origin, hash))
                .header(HEADER_SECRET, &self.secret)
                .body(Body::empty())?)
        };
        let (headers, _) = self.request_with_retry(make_req).await?;
        let len: u64 = headers
            .get(HEADER_FILE_LENGTH)
            .context("missing file length header")?
            .to_str()
            .map_err(anyhow::Error::from)?
            .parse()
            .map_err(anyhow::Error::from)?;
        Ok(len)
    }

    /// Read a range of bytes from a file.
    pub async fn get(&self, hash: &str, range: BlobRange) -> Result<BlobRead<'static>, Error> {
        let make_req = || async {
            let mut req = Request::builder()
                .method("GET")
                .uri(&format!("{}/{}", self.origin, hash))
                .header(HEADER_SECRET, &self.secret);
            if let Some((start, end)) = range {
                req = req.header(HEADER_RANGE, format!("{start}-{end}"));
            }
            Ok(req.body(Body::empty())?)
        };
        let (_, body) = self.request_with_retry(make_req).await?;
        Ok(BlobRead::from_stream(body_stream(body)))
    }

    /// Put a stream of data to the server, returning the hash ID if successful.
    pub async fn put<Fut, B>(&self, data: impl Fn() -> Fut) -> Result<String, Error>
    where
        Fut: Future<Output = anyhow::Result<B>>,
        B: Into<Body>,
    {
        let make_req = || async {
            Ok(Request::builder()
                .method("PUT")
                .uri(&self.origin)
                .header(HEADER_SECRET, &self.secret)
                .body(data().await?.into())?)
        };
        let (_, body) = self.request_with_retry(make_req).await?;
        let bytes = hyper::body::to_bytes(body)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(std::str::from_utf8(&bytes)
            .map_err(anyhow::Error::from)?
            .into())
    }
}
