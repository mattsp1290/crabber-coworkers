use clap::Parser;
use crabber_coworkers::app::{self, Cli};

fn main() {
    std::process::exit(app::run(&Cli::parse()));
}
