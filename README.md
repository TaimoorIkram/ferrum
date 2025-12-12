# The Ferrum Database Engine

Ferrum (_latin for Iron, hence the symbol Fe_) engine is a very small project aimed to be a fairly good implementation of a database management system.

## Main Features

Currently the engine is in its very initial days of development, but in the near future, I expect the engine to have the following features.

- A user friendly CLI interface with query processing.
- Feature-rich CLI tool.
- Quick data reads over large datasets with multi-threading.

## Project Structure

Structure is key and so currently, the project progresses in the following directory structure. These file names are placeholders only.

```bash
database-engine/
├── Cargo.toml
├── src/
│   ├── main.rs              # REPL / CLI entry point
│   ├── lib.rs               # Library root
│   ├── parser/              # SQL parsing
│   │   └── mod.rs
│   ├── storage/             # Storage engine
│   │   ├── mod.rs
│   │   ├── table.rs
│   │   └── row.rs
│   ├── execution/           # Query execution
│   │   ├── mod.rs
│   │   └── executor.rs
│   ├── concurrency/         # Threading, connection pooling
│   │   └── mod.rs
│   └── index/               # Indexing structures
│       └── mod.rs
└── tests/
    └── integration_tests.rs
```

## A `¯\_(ツ)_/¯` Moment

If you have any feature suggestion, feel free to leave them as an issue. Maybe I'll implement some of the easier ones, who knows...

*P.S. Rust corrodes Ferrum lol.*