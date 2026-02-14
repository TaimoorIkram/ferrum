```
███████╗███████╗██████╗ ██████╗ ██╗   ██╗███╗   ███╗
██╔════╝██╔════╝██╔══██╗██╔══██╗██║   ██║████╗ ████║
█████╗  █████╗  ██████╔╝██████╔╝██║   ██║██╔████╔██║
██╔══╝  ██╔══╝  ██╔══██╗██╔══██╗██║   ██║██║╚██╔╝██║
██║     ███████╗██║  ██║██║  ██║╚██████╔╝██║ ╚═╝ ██║
╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝ 
```

# Understanding the Internals

Ferrum is intended to be an extremely memory safe implementation of a relational database, but at a very small scale, in terms of features.

## 1. The `persistence` Module

At its core, you have the usual relational database concepts like `Databases`, `Tables`, `Rows`, `Columns`, and `Index`. All of these and some other smaller components make up the Persistence API. This is the heart of the project, responsible for in-memory manipulation of data.

All operations in the API are `non-atomic`, and I stress this because it means faulty rows in insert queries could cause the rest of the non-inserted rows to be lost. This will suffice for a simple implementation but not for deployment.

## 2. The `cli` Module

The engine uses the `clap` CLI tool to parse queries and send them to appropriate parts of the engine for processing. Currently, a simple 2-argument parser is implemented since aside from SQL processing, there isn't much else to implement. All commands that are not the part of the engine's own command set (only 2 commands for now; `help` and `corrode`) will be treated as SQL by default.

The SQL query is then passed to `sqlparser-rs` which creates the syntax tree for the query and allows the engine to implement an AST processor to run the statement.

## 3. The `sessions` Module (WIP)

Information regarding the current user session, such as command history and snapshots, will be kept here. Nothing is confirmed for this section yet as it will soon be implemented.

## 4. The `functions` Module

Provides a set of operations to customize how you see your data. The fundamental classification of functions is among two groups: `scalars` and `aggregators` depending on how they interact with the rows of the table.

# Module Interconnection

The `main` thread starts a `client` REPL. Input commands are passed to the `CLI Parser` to check if it is an internal command, from where it moves to the `SQL Parser` if the statement is not an internal command.

`SQL Parser` creates an AST that is processed and operations are performed using the persistence module.

---
`A tiny little database engine project.` \
_&copy; 2026 Ferrum Engine_