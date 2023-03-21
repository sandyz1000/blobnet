/// Quick benchmark for testing the latency of a single repeated read
/// from blobnet.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,

    /// File size in bytes.
    file_size_bytes: u32,

    /// Number of iterations.
    iterations: u64,

    /// Connections to use
    connections: u32,

    /// Request concurrency to use
    concurrency: u64,
}

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use blobnet::{client::FileClient, drain, Error};
use clap::Parser;
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data = str::repeat("abcdefghijklmnop", (args.file_size_bytes / 16_u32) as usize);

    let client = Arc::new(FileClient::new_http_with_pool(
        &args.origin,
        &args.secret,
        args.connections,
    ));
    let hash = format!("{:x}", Sha256::new().chain_update(&data).finalize());
    match client.head(&hash).await {
        Ok(n) => assert_eq!(n, data.len() as u64),
        Err(Error::NotFound) => {
            client.put(|| async { Ok(data.clone()) }).await?;
            println!("wrote {} bytes: hash={}", data.len(), hash);
        }
        Err(e) => panic!("{}", e),
    }

    let n = drain(client.get(&hash, None).await?).await?;
    println!("read {} bytes", n);

    let mut times = vec![];
    for _ in 0..args.iterations {
        let start = Instant::now();
        let m = n / args.concurrency;
        let mut set = JoinSet::new();
        for i in 0..args.concurrency {
            let c = client.clone();
            let h = hash.clone();
            set.spawn(async move {
                let r = Some((i * m, ((i + 1) * m)));
                let r = c.get(&h, r).await.unwrap();
                drain(r).await.unwrap()
            });
        }
        while let Some(res) = set.join_next().await {
            let pn: u64 = res?;
            assert_eq!(pn, m);
        }
        times.push(start.elapsed().as_micros());
    }
    let avg_latency = times.iter().sum::<u128>() as f64 / args.iterations as f64;
    let avg_thpt = (8 * n) as f64 / (1 << 30) as f64 / (avg_latency / 1_000_000_f64);
    println!("avg = {} us", avg_latency);
    println!("thpt = {:.2} Gbit/s", avg_thpt);
    Ok(())
}
