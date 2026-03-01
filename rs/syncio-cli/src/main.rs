use std::path::PathBuf;

use clap::Parser;

/// A CLI version for running syncio
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliConfig {
    directory: PathBuf,
    peers: Vec<iroh::EndpointId>,
}

fn main() {}
