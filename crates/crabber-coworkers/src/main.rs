use clap::Parser;
use crabber_coworkers::app::{self, Cli};

#[tokio::main]
async fn main() {
    std::process::exit(app::run(Cli::parse()).await);
}
