//! Providers used for testing and internal benchmarks.
//!
//! These providers are not meant to be used by consumers of the library and may
//! be removed or changed at any time.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{ready, Poll};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::{provider::Provider, BlobRange, Error, ReadStream};

/// Tracks network out bytes served by `get` requests.
pub struct Tracking<P> {
    inner: P,
    pub get_net_bytes_served: Arc<AtomicUsize>,
}

impl<P> Tracking<P> {
    /// Create a new `Tracking` provider.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            get_net_bytes_served: AtomicUsize::new(0).into(),
        }
    }

    /// Wrap a stream, introducing tracking of bytes sent.
    fn wrap_tracking<'a>(&self, data: ReadStream<'a>) -> ReadStream<'a> {
        Box::pin(TrackingStream::new(data, self.get_net_bytes_served.clone()))
    }
}

#[async_trait]
impl<P: Provider> Provider for Tracking<P> {
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        self.inner.head(hash).await
    }

    async fn get(&self, hash: &str, range: BlobRange) -> Result<ReadStream<'static>, Error> {
        let result = self.inner.get(hash, range).await;
        Ok(self.wrap_tracking(result?))
    }

    async fn put(&self, data: ReadStream<'_>) -> Result<String, Error> {
        self.inner.put(data).await
    }
}

/// Adds random delay to all operations of a provider.
///
/// The components of delay before and after the operation are independent and
/// identically distributed. If this is a random variable X, then a
/// representation for the distribution of X is given by
///
/// `X ~ mean(X) * (0.2 + 0.8 * Expo)`
///
/// Where `Expo` is an exponential distribution with mean 1.
pub struct Delayed<P> {
    inner: P,
    base_delay: f64,
    network_gbps: f64,
}

impl<P> Delayed<P> {
    /// Create a new `Delayed` provider with the given mean delay (s) and
    /// simulated network throughput (Gbps).
    pub fn new(inner: P, base_delay: f64, network_gbps: f64) -> Self {
        assert!(base_delay >= 0.0 && base_delay.is_finite());
        assert!(network_gbps > 0.0);
        Self {
            inner,
            base_delay,
            network_gbps,
        }
    }

    /// Wrap a stream, introducing random throughput-dependent delays.
    fn wrap_throttle<'a>(&self, data: ReadStream<'a>) -> ReadStream<'a> {
        Box::pin(NetworkStream::new(data, self.network_gbps))
    }
}

#[async_trait]
impl<P: Provider> Provider for Delayed<P> {
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        wait(0.5 * self.base_delay).await;
        let result = self.inner.head(hash).await;
        wait(0.5 * self.base_delay).await;
        result
    }

    async fn get(&self, hash: &str, range: BlobRange) -> Result<ReadStream<'static>, Error> {
        wait(0.5 * self.base_delay).await;
        let result = self.inner.get(hash, range).await;
        wait(0.5 * self.base_delay).await;
        Ok(self.wrap_throttle(result?))
    }

    async fn put(&self, data: ReadStream<'_>) -> Result<String, Error> {
        wait(0.5 * self.base_delay).await;
        let result = self.inner.put(self.wrap_throttle(data)).await;
        wait(0.5 * self.base_delay).await;
        result
    }
}

struct NetworkStream<'a> {
    inner: ReadStream<'a>,
    network_gbps: f64,
    waiting: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

impl<'a> NetworkStream<'a> {
    fn new(inner: ReadStream<'a>, network_gbps: f64) -> Self {
        Self {
            inner,
            network_gbps,
            waiting: None,
        }
    }
}

impl AsyncRead for NetworkStream<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if let Some(waiting) = &mut this.waiting {
            ready!(waiting.as_mut().poll(cx));
            this.waiting = None;
        }

        let len = buf.filled().len();
        ready!(this.inner.as_mut().poll_read(cx, buf))?;
        let bytes_sent = buf.filled().len() - len;
        if bytes_sent > 0 {
            let delay = (bytes_sent as f64) / (this.network_gbps * 1e9 / 8.0);
            this.waiting = Some(Box::pin(wait(delay)));
        }
        Poll::Ready(Ok(()))
    }
}

/// Wait for a random duration with given mean.
async fn wait(mut mean: f64) {
    if mean <= 0.0 {
        return;
    }
    const MIN_SLEEP_SECS: f64 = 500e-6;
    if mean <= MIN_SLEEP_SECS {
        // If less than minimum duration, randomly sample.
        if fastrand::f64() * MIN_SLEEP_SECS < mean {
            mean = MIN_SLEEP_SECS;
        } else {
            return;
        }
    }

    let expo = (-fastrand::f64().ln()).min(100.0);
    let delay = mean * (0.5 + 0.5 * expo);
    // This is needed because `tokio::time::sleep` uses epoll, which only has
    // millisecond-granularity.
    tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_secs_f64(delay));
    })
    .await
    .unwrap();
}

struct TrackingStream<'a> {
    inner: ReadStream<'a>,
    bytes_read: Arc<AtomicUsize>,
}

impl<'a> TrackingStream<'a> {
    fn new(inner: ReadStream<'a>, bytes_read: Arc<AtomicUsize>) -> Self {
        Self { inner, bytes_read }
    }
}

impl AsyncRead for TrackingStream<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        let len = buf.filled().len();
        ready!(this.inner.as_mut().poll_read(cx, buf))?;
        let bytes_sent = buf.filled().len() - len;
        this.bytes_read.fetch_add(bytes_sent, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}
