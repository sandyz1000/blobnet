/// Quick benchmark for testing the latency of a single, small repeated read
/// from blobnet.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,
}

use std::time::Instant;

use anyhow::Result;
use blobnet::{client::FileClient, read_to_vec};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data = str::repeat("abcdefghijklmnop", 4096); // 64 KiB

    let client = FileClient::new_http(&args.origin, &args.secret);
    let hash = client.put(|| async { Ok(data.clone()) }).await?;

    let output = read_to_vec(client.get(&hash, None).await?).await?;
    println!("read {} bytes", output.len());

    let mut times = vec![];
    for _ in 0..10000 {
        let start = Instant::now();
        let output2 = read_to_vec(client.get(&hash, None).await?).await?;
        times.push(start.elapsed().as_micros());
        assert!(output2.len() == output.len());
    }
    println!("avg = {} us", times.iter().sum::<u128>() as f64 / 10000.0);
    Ok(())
}
