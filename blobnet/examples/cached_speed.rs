/// Benchmark for testing the latency of a single repeated read
/// from a blobnet server to a caching blobnet client.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,

    /// File size in bytes.
    file_size_bytes: u32,

    /// Number of iterations.
    iterations: u16,

    /// Connection to use
    connections: u32,

    /// Prefetch depth
    prefetch_depth: u32,
}

use std::time::Instant;

use anyhow::Result;
use blobnet::provider::{Cached, Provider, Remote};
use blobnet::{client::FileClient, drain, Error};
use clap::Parser;
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;

fn main() -> Result<()> {
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::new();

    let runtime = Runtime::new()?;
    runtime.block_on(run(&mut tempdirs))?;
    drop(runtime);

    println!("cleaning up directories");
    for dir in tempdirs {
        dir.close()?;
    }

    Ok(())
}

async fn run(tempdirs: &mut Vec<tempfile::TempDir>) -> Result<()> {
    let args = Args::parse();

    let data = str::repeat("abcdefghijklmnop", (args.file_size_bytes / 16_u32) as usize);

    let file_client = FileClient::new_http_with_pool(&args.origin, &args.secret, args.connections);

    let hash = format!("{:x}", Sha256::new().chain_update(&data).finalize());
    match file_client.head(&hash).await {
        Ok(n) => assert_eq!(n, data.len() as u64),
        Err(Error::NotFound) => {
            file_client.put(|| async { Ok(data.clone()) }).await?;
            println!("wrote {} bytes: hash={}", data.len(), hash);
        }
        Err(e) => panic!("{}", e),
    }

    let pagesize = 1 << 21;

    // Do one test read.
    let file_client = FileClient::new_http_with_pool(&args.origin, &args.secret, args.connections);
    let remote = Remote::new(file_client);
    let dir = tempfile::tempdir().unwrap();
    let mut cached = Cached::new(remote, &dir, pagesize);
    cached.set_prefetch_depth(args.prefetch_depth);
    tempdirs.push(dir);
    let n = drain(cached.get(&hash, None).await?).await?;
    println!("read {} bytes", n);

    let mut times = vec![];
    for _ in 0..args.iterations {
        // New cached provider used each time, pointing at a new disk cache directory.
        let file_client = FileClient::new_http(&args.origin, &args.secret);
        let remote = Remote::new(file_client);
        let dir = tempfile::tempdir().unwrap();
        let mut cached = Cached::new(remote, &dir, pagesize);
        cached.set_prefetch_depth(args.prefetch_depth);
        tempdirs.push(dir);

        let start = Instant::now();
        let n2 = drain(cached.get(&hash, None).await?).await?;
        times.push(start.elapsed().as_micros());
        assert_eq!(n2, n);
    }
    let avg_latency = times.iter().sum::<u128>() as f64 / args.iterations as f64;
    let avg_thpt = (8 * n) as f64 / (1 << 30) as f64 / (avg_latency / 1_000_000_f64);
    println!("avg = {} us", avg_latency);
    println!("thpt = {:.2} Gbit/s", avg_thpt);

    Ok(())
}
