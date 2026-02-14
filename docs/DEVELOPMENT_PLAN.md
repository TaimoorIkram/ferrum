```
███████╗███████╗██████╗ ██████╗ ██╗   ██╗███╗   ███╗
██╔════╝██╔════╝██╔══██╗██╔══██╗██║   ██║████╗ ████║
█████╗  █████╗  ██████╔╝██████╔╝██║   ██║██╔████╔██║
██╔══╝  ██╔══╝  ██╔══██╗██╔══██╗██║   ██║██║╚██╔╝██║
██║     ███████╗██║  ██║██║  ██║╚██████╔╝██║ ╚═╝ ██║
╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝ 
```

# The Development Plan

I don't have much in mind for now, except that I've always been curious to build a system that can store and retrieve data. I tried making such a project 2 years ago in Java, but let's just say, it didn't end well.

## Intended Features

I consider the development to be good enough when I check the following boxes. These are all minimum goals I intend to achieve before I add anything unique to it or experiment with the code quality.

1. A feature-rich command line tool with SQL processing.
2. A persistence engine that supports about 70% of the working of traditional relational database engines.
3. The engine can be run as a server on some cloud instance.
4. Basic file reading and writing to store the data and load it.

## Future Plan

After the base code is properly set and is running fine, I might try making it multi-threaded. This doesn't mean I'll keep all processes single-threaded; I may apply multi-threading in some places during the development of the intended features.

---
`A tiny little database engine project.` \
_&copy; 2026 Ferrum Engine_