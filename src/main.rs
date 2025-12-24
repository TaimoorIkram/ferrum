use clap::Parser;
use ferrum_engine::cli::{
    self,
    parsers::{CliMode, CliParser, SqlParser},
};
use sqlparser::dialect::GenericDialect;

fn main() {
    // let sql = "SELECT * FROM table1";
    // let dialect = GenericDialect {};
    // let ast = SqlParser::parse_sql(&dialect, sql);

    // println!("{:#?}", ast);

    let args = CliParser::parse();
    // let args: Vec<String> = env::args().collect();

    let mode = args
        .mode
        .expect("usage: please specify a mode: client/server");

    match mode {
        CliMode::Client => cli::run_client(),
        CliMode::Server => cli::run_server(),
    }
}
