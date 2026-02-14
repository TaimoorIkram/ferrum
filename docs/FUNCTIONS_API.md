```
███████╗███████╗██████╗ ██████╗ ██╗   ██╗███╗   ███╗
██╔════╝██╔════╝██╔══██╗██╔══██╗██║   ██║████╗ ████║
█████╗  █████╗  ██████╔╝██████╔╝██║   ██║██╔████╔██║
██╔══╝  ██╔══╝  ██╔══██╗██╔══██╗██║   ██║██║╚██╔╝██║
██║     ███████╗██║  ██║██║  ██║╚██████╔╝██║ ╚═╝ ██║
╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝ 
```
# The Functions API

The functions API is the place where all the operations lie, that create data, not present in the original table. In other words, these operations provide us a way to visualize data in ways, that the persistence API alone can never.

It contains two main parts based on what they do to the tables that they are performed on.

## General Working Mechanism

The modules inside this API follow the resgistry pattern. The main mod.rs file contains a runner or run-fetcher that looks for an item inside its registry. If found that item is then returned and an error is thrown otherwise.

## Scalars

Scalars are used to perform operations on a particular column of a table. For example, look at the following query:
```sql
SELECT name, age, ADD(age, 50) as "Plus 50" FROM people;
```

The query above will select `name` and `age` from the original table and create a [`TableReader`] on it. After this, it will create a new column on this reader where every row will be appended with a new cell that contains the value of its respective `age` and 50 added to it.

The end result is a table with three columns shown to the command line user. An alias (in our case `Plus 50`) otherwise the name of the function in CAPS will show as the column name.

### Adding a new scalar

To create a new scalar function, follow these steps:

1. Create a new file inside the `scalars/` folder called `<scalar>.rs` where the placeholder is the name of the scalar. 
2. Add code to the file, based on the already added scalar files and register the scalar inside the `scalars/mod.rs`.

> *Do not forget to set the `SCLR_NAME` as it is a crucial part to registering your scalar inside the resolver function.*

## Aggregators

Aggregators are used to perform operations on the whole table at once and returns a single cell as a result. For example, look at the following query:
```sql
SELECT COUNT(*) AS "Total", MAX(age) AS "Oldest", MIN(age) AS "Youngest" FROM people;
```

The query above will select the table `people` from the current database and create an empty [`TableReader`] on it. After this, it will create a new column on this reader where the single value result of the functions above, will be stored.

The end result is a table with three columns shown to the command line user. An alias (in our case `Total`, `Oldest`, and `Youngest`) otherwise the name of the function in CAPS will show as the column name.

### Adding a new aggregator

To create a new aggregator function, follow these steps:

1. Create a new file inside the `aggregators/` folder called `<aggregator>.rs` where the placeholder is the name of the aggregator. 
2. Add code to the file, based on the already added aggregator files and register the aggregator inside the `aggregators/mod.rs`.

> *Do not forget to set the `AGGR_NAME` as it is a crucial part to registering your aggregator inside the resolver function.*

---
`A tiny little database engine project.` \
_&copy; 2026 Ferrum Engine_