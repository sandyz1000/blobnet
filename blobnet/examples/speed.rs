/// Quick benchmark for testing the latency of a single repeated read
/// from blobnet.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,

    /// File size in bytes.
    file_size_bytes: u64,

    /// Number of iterations.
    iterations: u32,

    /// Connections to use
    #[clap(long)]
    connections: u32,

    /// Request concurrency to use
    #[clap(long)]
    concurrency: u64,

    /// Page size to use when fetching. Default is to fetch the entire file in
    /// one request. Configuring this page size will cause the file to be
    /// fetched in multiple sub-ranges.
    #[clap(long)]
    page_size: Option<u64>,

    /// Hash to fetch
    #[clap(long)]
    file_hash: Option<String>,

    /// Enable disk caching with a new temp directory per iteration
    #[clap(long, group("caching"))]
    cache: bool,

    /// Enable disk caching using this directory
    #[clap(long, group("caching"))]
    cache_dir: Option<String>,

    /// Configure cache page size. Default is same as the fetching page size.
    #[clap(long)]
    cache_page_size: Option<u64>,
}

use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::iter::Sum;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use blobnet::provider::{Cached, Provider, Remote};
use blobnet::{client::FileClient, drain, Error};
use clap::Parser;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::task;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = FileClient::new_http_with_pool(&args.origin, &args.secret, args.connections);

    let hash = match args.file_hash {
        Some(hash) => hash,
        None => {
            let data = str::repeat("abcdefghijklmnop", (args.file_size_bytes / 16) as usize);
            let hash = format!("{:x}", Sha256::new().chain_update(&data).finalize());
            match client.head(&hash).await {
                Ok(n) => assert_eq!(n, data.len() as u64),
                Err(Error::NotFound) => {
                    client.put(|| async { Ok(data.clone()) }).await?;
                    println!("wrote {} bytes: hash={}", data.len(), hash);
                }
                Err(e) => panic!("{}", e),
            }
            hash
        }
    };

    let fetch_page_size_bytes = args.page_size.unwrap_or(args.file_size_bytes);
    let cache_page_size_bytes = args.cache_page_size.unwrap_or(fetch_page_size_bytes);

    let mut times = vec![];
    for i in 1..=args.iterations {
        let remote = Remote::new(client.clone());
        let (blobnet, cache, tmpdir): (Arc<dyn Provider>, _, _) =
            match (args.cache, args.cache_dir.clone()) {
                (false, None) => (Arc::new(remote), None, None),
                (true, _) => {
                    let dir = tempdir().unwrap();
                    let cached = Arc::new(Cached::new(remote, &dir, cache_page_size_bytes));
                    (cached.clone(), Some(cached), Some(dir))
                }
                (_, Some(dir)) => {
                    let cached = Arc::new(Cached::new(remote, dir, cache_page_size_bytes));
                    (cached.clone(), Some(cached), None)
                }
            };

        let start = Instant::now();
        let n = fetch(
            &blobnet,
            &hash,
            args.file_size_bytes as u64,
            args.concurrency,
            fetch_page_size_bytes,
        )
        .await?;
        let latency = start.elapsed();
        let thpt = throughput_mibps(n, latency);
        times.push(latency);

        println!();
        println!("iteration {}", i);
        println!("lat = {} us", latency.as_micros());
        println!("thpt = {:.2} Gbit/s ({:.2} GB/s)", thpt, thpt / 8f64);

        if let Some(cache) = cache {
            let stats = cache.stats();
            let pending_disk_write_pages_mb =
                stats.pending_disk_write_bytes as f64 / (1 << 20) as f64;
            if stats.pending_disk_write_pages > 0 {
                print!(
                    "waiting for {} ({} MB) pending cache writes..",
                    stats.pending_disk_write_pages, pending_disk_write_pages_mb
                );
                for j in 0.. {
                    if cache.stats().pending_disk_write_pages == 0 {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                    if j % 100 == 0 {
                        print!(".");
                        io::stdout().flush().unwrap();
                    }
                }
            }
            println!("done");

            if let Some(tmpdir) = tmpdir {
                tmpdir.close()?;
            }

            let latency = start.elapsed();
            let thpt = throughput_mibps(n, latency);
            println!(
                "disk write thpt = {:.2} Gbit/s ({:.2} GB/s)",
                thpt,
                thpt / 8f64
            );
        }
    }
    if args.iterations > 0 {
        let tot_latency = Duration::sum(times.iter());
        let avg_latency = tot_latency / args.iterations;
        let avg_thpt = throughput_mibps(args.file_size_bytes, avg_latency);
        println!();
        println!("finished");
        println!("avg = {} us", avg_latency.as_micros());
        println!(
            "thpt = {:.2} Gbit/s ({:.2} GB/s)",
            avg_thpt,
            avg_thpt / 8f64
        );
    }
    Ok(())
}

fn throughput_mibps(size: u64, latency: Duration) -> f64 {
    (8 * size) as f64 / (1 << 30) as f64 / (latency.as_micros() as f64 / 1_000_000_f64)
}

async fn fetch(
    client: &Arc<dyn Provider>,
    hash: &str,
    file_size_bytes: u64,
    concurrency: u64,
    page_size: u64,
) -> Result<u64> {
    let mut tasks = VecDeque::with_capacity(concurrency as usize);
    let mut bytes_read = 0;
    let pages = file_size_bytes / page_size;
    let hash = hash.to_string();
    for i in 0..pages {
        let client = client.clone();
        let hash = hash.clone();
        if i >= concurrency {
            bytes_read += tasks.pop_front().unwrap().await?;
        }
        let task = task::spawn(async move {
            let range = Some((i * page_size, ((i + 1) * page_size)));
            let stream = client.get(&hash, range).await.unwrap();
            drain(stream).await.unwrap()
        });
        tasks.push_back(task);
    }
    while !tasks.is_empty() {
        bytes_read += tasks.pop_front().unwrap().await?;
    }
    Ok(bytes_read)
}
