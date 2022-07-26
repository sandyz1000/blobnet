//! This is a quick benchmark for testing the latency of a single repeated read
//! from blobnet.
//!
//! To use this script, run like the following example:
//!
//! ```bash
//! cargo run --release --example speed \
//!     http://172.31.21.36:7609 \
//!     002763c21dd2eaad246b0fd336b045d4965366cbb45e375d277b5e544ae3e8f9 \
//!     auth-secret
//! ```
//!
//! The first argument is the address of the blobnet server, and the second
//! argument is the hash of a file to retrieve from the server.
//!
//! The third argument is the authentication secret.

use std::{env, time::Instant};

use anyhow::Result;
use blobnet::FileClient;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    assert!(
        args.len() == 4,
        "usage: {} <origin> <hash> <secret>",
        args[0],
    );

    let origin = &args[1];
    let hash = &args[2];
    let secret = &args[3];

    let client = FileClient::new(origin, secret);
    let output = client.get(hash, None).await?;
    println!("read {} bytes", output.len());

    let mut times = vec![];
    for _ in 0..10000 {
        let start = Instant::now();
        let output2 = client.get(hash, None).await?;
        times.push(start.elapsed().as_micros());
        assert!(output2.len() == output.len());
    }
    println!("avg = {} us", times.iter().sum::<u128>() as f64 / 10000.0);
    Ok(())
}
