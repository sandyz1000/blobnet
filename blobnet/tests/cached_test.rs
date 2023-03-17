use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::Result;
use blobnet::provider;
use blobnet::provider::Provider;
use blobnet::server::{listen, Config};
use blobnet::test_provider;
use blobnet::{client::FileClient, read_to_vec};
use hyper::client::HttpConnector;
use hyper::server::conn::AddrIncoming;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::task;

type TrackingProvider = test_provider::Tracking<provider::Remote<HttpConnector>>;

/// Spawn a temporary file server on localhost, only used for testing.
async fn spawn_temp_server() -> Result<Arc<TrackingProvider>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut incoming = AddrIncoming::from_listener(listener)?;
    incoming.set_nodelay(true);
    tokio::spawn(async {
        let dir = tempdir().unwrap();
        let provider = provider::LocalDir::new(dir.path().join("nfs"));
        let config = Config {
            provider: Box::new(provider),
            secret: "secret".into(),
        };
        listen(config, incoming).await.unwrap();
    });

    let client = FileClient::new_http(&format!("http://{addr}"), "secret");
    let remote = provider::Remote::new(client);
    let tracking = test_provider::Tracking::new(remote);
    Ok(Arc::new(tracking))
}

#[tokio::test]
async fn concurrent_cacheable_reads() -> Result<()> {
    let tracking_client = spawn_temp_server().await?;
    let dir = tempdir().unwrap();
    // Create a caching provider that should only forward one GET request to the
    // underlying tracking test provider.
    let cached_provider = Arc::new(provider::Cached::new(
        tracking_client.clone(),
        dir.path().join("cache"),
        1 << 21,
    ));

    let s1 = "hello world!";
    let data1 = Box::pin(s1.as_bytes());
    let h1 = tracking_client.put(data1).await?;

    // Create hundreds of concurrent reads on the same hash.
    let num_concurrent = 1 << 10;
    let mut set = task::JoinSet::new();
    for _ in 0..num_concurrent {
        let h1 = h1.clone();
        let client = cached_provider.clone();
        set.spawn(async move {
            let stream = (client.get(&h1, None).await).unwrap();
            let _ = read_to_vec(stream).await.unwrap();
        });
    }
    // Wait for all get requests to finish.
    while set.join_next().await.is_some() {}
    // TODO(Jonathon): Deduplication is currently not perfect. There's still races
    // where multiple requests hit the underlying provider before the Cached
    // provider's page cache and disk cache are populated.
    // So, for now, we set a max tolerance.
    let max_bytes_tolerated = s1.len() * 16;
    let total_net_out_bytes = tracking_client.get_net_bytes_served.load(Ordering::SeqCst);
    assert!(
        total_net_out_bytes <= max_bytes_tolerated,
        "duplicated network out bytes exceeded tolerance: {total_net_out_bytes} > \
         {max_bytes_tolerated}",
    );
    Ok(())
}
