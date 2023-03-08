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
}

use std::time::Instant;

use anyhow::Result;
use blobnet::provider::{Cached, Provider, Remote};
use blobnet::{client::FileClient, read_to_vec};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data = str::repeat("abcdefghijklmnop", (args.file_size_bytes / 16_u32) as usize);

    let file_client = FileClient::new_http(&args.origin, &args.secret);

    println!("putting {} bytes", data.len());
    let hash = file_client.put(|| async { Ok(data.clone()) }).await?;

    let pagesize = 1 << 21;

    let mut tempdirs: Vec<tempfile::TempDir> = Vec::new();

    // Do one test read.
    let file_client = FileClient::new_http(&args.origin, &args.secret);
    let remote = Remote::new(file_client);
    let dir = tempfile::tempdir().unwrap();
    let cached = Cached::new(remote, &dir, pagesize);
    tempdirs.push(dir);
    let output = read_to_vec(cached.get(&hash, None).await?).await?;
    println!("read {} bytes", output.len());

    let mut times = vec![];
    for _ in 0..args.iterations {
        // New cached provider used each time, pointing at a new disk cache directory.
        let file_client = FileClient::new_http(&args.origin, &args.secret);
        let remote = Remote::new(file_client);
        let dir = tempfile::tempdir().unwrap();
        let cached = Cached::new(remote, &dir, pagesize);
        tempdirs.push(dir);

        let start = Instant::now();
        let output2 = read_to_vec(cached.get(&hash, None).await?).await?;
        times.push(start.elapsed().as_micros());
        assert!(output2.len() == output.len());
    }
    println!(
        "avg = {} us",
        times.iter().sum::<u128>() as f64 / args.iterations as f64
    );

    println!("cleaning up directories");
    for dir in tempdirs {
        dir.close()?;
    }

    Ok(())
}
