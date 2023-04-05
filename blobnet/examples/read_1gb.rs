use std::path::PathBuf;
use std::time::Instant;

use blobnet::client::FileClient;
use blobnet::provider::{self, Provider};
use clap::Parser;
use tikv_jemallocator::Jemalloc;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

/// Try to insert and read 1 GB from a blobnet server.
///
/// This uses the caching provider, which actually reads the file in chunks and
/// saves those in temporary files on the current instance.
///
/// Performs a throughput calculation based on different time components.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,

    /// Folder to put the temporary cache in.
    #[clap(short, long, value_parser)]
    cache_dir: Option<PathBuf>,
}

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

async fn run_test(
    label: &str,
    bytes: u64,
    hash: &str,
    provider: impl Provider,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let data = blobnet::read_to_bytes(provider.get(hash, Some((0, bytes))).await?).await?;
    let elapsed = start.elapsed();
    println!("read {label} in {elapsed:?}");
    println!(
        " ┗━━ throughput: {:.6} GiB/s",
        bytes as f64 / (1 << 30) as f64 / elapsed.as_secs_f64()
    );
    assert_eq!(data.len() as u64, bytes);
    assert_eq!(data[0], b'a');
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dir = match &args.cache_dir {
        Some(dir) => tempfile::tempdir_in(dir)?,
        None => tempfile::tempdir()?,
    };

    let client = FileClient::new_http(&args.origin, &args.secret);
    let provider = provider::Remote::new(client);
    let provider = provider::Cached::new(provider, dir.path(), 1 << 21);

    let rt = Runtime::new()?;

    rt.block_on(async {
        let start = Instant::now();
        let hash = provider
            .put(Box::pin(tokio::io::repeat(b'a').take(1 << 30)))
            .await?;
        println!("inserted 1gb of 'a' in {:?}", start.elapsed());

        println!("\n-- First run: cold");
        run_test("64 KiB", 1 << 16, &hash, &provider).await?;
        run_test("4 MiB", 1 << 22, &hash, &provider).await?;
        run_test("64 MiB", 1 << 26, &hash, &provider).await?;
        run_test("1 GiB", 1 << 30, &hash, &provider).await?;

        println!("\n-- Second run: locally cached");
        run_test("64 KiB", 1 << 16, &hash, &provider).await?;
        run_test("4 MiB", 1 << 22, &hash, &provider).await?;
        run_test("64 MiB", 1 << 26, &hash, &provider).await?;
        run_test("1 GiB", 1 << 30, &hash, &provider).await?;

        println!("\n-- Third run");
        run_test("64 KiB", 1 << 16, &hash, &provider).await?;
        run_test("4 MiB", 1 << 22, &hash, &provider).await?;
        run_test("64 MiB", 1 << 26, &hash, &provider).await?;
        run_test("1 GiB", 1 << 30, &hash, &provider).await?;

        anyhow::Ok(())
    })?;

    Ok(())
}
