# blobnet

[![Latest Version](https://img.shields.io/crates/v/blobnet.svg)](https://crates.io/crates/blobnet)
[![API Documentation](https://docs.rs/blobnet/badge.svg)](https://docs.rs/blobnet)

A low-latency file server that responds to requests for chunks of file data.

This acts as a non-volatile, over-the-network content cache. Internal users can
add binary blobs to the cache, and the data is indexed by its SHA-256 hash. Any
blob can be retrieved by its hash and range of bytes to read.

Data stored in this server is locally cached, backed by NFS, and durable.

## Usage

Run `cargo install blobnet` and see the options in the CLI. The server supports
only two types of requests (`GET` / `PUT`), and an example client in Rust is
located in the `blobnet` library crate.
