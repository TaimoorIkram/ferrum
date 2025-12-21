use ferrum_engine::cli;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if let Some(mode) = args.get(1) {
        match mode.as_str() {
            "client" => cli::run_client(),
            "server" => cli::run_server(),
            _ => println!("invalid mode: {} is not a mode", mode),
        }

        println!("goodbye!")
    } else {
        println!("usage: ferrum <client/server>")
    }

}
