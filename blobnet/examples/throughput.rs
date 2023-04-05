use anyhow::Result;
use blobnet::{client::FileClient, read_to_bytes};
use clap::Parser;
use tikv_jemallocator::Jemalloc;
use tokio::task;

/// Create client connections against a blobnet server. Useful for measuring
/// network throughput.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,

    /// Number of clients to use.
    n_tasks: u32,
}

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("target => {}", args.origin);

    // Send data to blobnet.
    let data = str::repeat("abcdefghijklmnop", 4096 * 100);
    let client = FileClient::new_http(&args.origin, &args.secret);
    let hash = client.put(|| async { Ok(data.clone()) }).await?;

    // Create clients as background tasks.
    let mut set = task::JoinSet::new();
    for _ in 0..args.n_tasks {
        set.spawn(repeat_requests(
            args.origin.clone(),
            args.secret.clone(),
            hash.clone(),
        ));
    }
    println!("spawned tasks => {}", args.n_tasks);
    set.join_next().await;
    Ok(())
}

// Makes continuous requests to a Blobnet server.
async fn repeat_requests(origin: String, secret: String, hash: String) -> Result<()> {
    let client = FileClient::new_http(&origin, &secret);
    loop {
        read_to_bytes(client.get(&hash, None).await?).await?;
    }
}
