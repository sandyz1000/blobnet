//! This creates client connections against a blobnet server. Useful
//! for measuring network throughput.
//!
//! To use this script, run like the following example:
//!
//! ```bash
//! cargo run --release --example throughput http://127.0.0.1:7609 secret
//! ```
//!
//! The first argument is the address of the blobnet server, and the second
//! argument is the authentication secret.
//!
//! The third argument is the number of clients to use.

use std::env;

use anyhow::Result;
use blobnet::{client::FileClient, read_to_vec};
use hyper::client::HttpConnector;
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    assert!(
        args.len() == 4,
        "usage: {} <origin> <secret> <n_tasks>",
        args[0]
    );

    let origin = &args[1];
    let secret = &args[2];
    let n_tasks: &u32 = &args[3].parse().unwrap();

    println!("target => {}", origin);

    // Send data to blobnet.
    let data = str::repeat("abcdefghijklmnop", 4096 * 100);
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    let client = FileClient::new(connector, &origin, &secret);
    let hash = client.put(|| async { Ok(data.clone()) }).await?;

    // Create clients as background tasks.
    let mut set = task::JoinSet::new();
    for _ in 0..*n_tasks {
        set.spawn(repeat_requests(
            origin.clone(),
            secret.clone(),
            hash.clone(),
        ));
    }
    println!("spawned tasks => {}", n_tasks);
    set.join_next().await;
    Ok(())
}

// Makes continuous requests to a Blobnet server.
async fn repeat_requests(origin: String, secret: String, hash: String) -> Result<()> {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    let client = FileClient::new(connector, &origin, &secret);

    loop {
        read_to_vec(client.get(&hash, None).await?).await?;
    }
}
