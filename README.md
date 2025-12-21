# The Ferrum Database Engine

Ferrum (_latin for Iron, hence the symbol Fe_) engine is a very small project aimed to be a fairly good implementation of a database management system.

## Main Features

Currently the engine is in its very initial days of development, but in the near future, I expect the engine to have the following features.

- A user friendly CLI interface with query processing.
- Feature-rich CLI tool.
- Quick data reads over large datasets with multi-threading.

## The `Issues` Section in Doc Comments

Throughout the code, you will see comments, regarding design descriptions, which are a fade overview of my intent to implement things in the structs and methods. In those comments, there is a specific section I want to throw some light on and that is the "Issues" section.

The "Issues" section is written not to point out defects, but to indicate the future improvements so I do not forget. They will, at some point in time, be resolved and integrated into the system. Right now, building a foundation is more important. Having a list of what else to look forward to, in addition to future design bugs coming out of the foundation design, it is important to keep a list of these things by the side. That is exactly what this section is about.

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