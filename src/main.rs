use clap::Parser;
use ferrum_engine::{
    cli::{
        self,
        parsers::{CliMode, CliParser},
    },
    config::EngineConfig,
};

fn main() {
    let args = CliParser::parse();
    // let args: Vec<String> = env::args().collect();

    let config = EngineConfig::new();

    let mode = args
        .mode
        .expect("usage: please specify a mode: client/server");

    match mode {
        CliMode::Client => cli::run_client(&config),
        CliMode::Server => cli::run_server(),
    }
}
