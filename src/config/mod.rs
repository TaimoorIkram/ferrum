use dotenvy::dotenv;
use std::env;

pub struct EngineConfig {
    pub registry_file_path: String,
}

impl EngineConfig {
    pub fn new() -> EngineConfig {
        dotenv().ok();

        fn get(key: &str) -> String {
            env::var(key).unwrap_or_else(|_| panic!("Missing env var: {}", key))
        }

        EngineConfig {
            registry_file_path: get("FERRUM_REGISTRY_FILE"),
        }
    }
}
