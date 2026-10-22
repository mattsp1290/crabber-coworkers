use clap::Parser;

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {}

#[must_use]
pub fn run(_cli: &Cli) -> i32 {
    0
}
