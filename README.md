# My thoughts on DuckDB and R with examples

R packages discussed in these notes include duckdb (of course), dplyr,
data.table, fst, xts, RSQLite, and vroom and a little Python Pandas by way of
reticulate.

The notes exhibit something of a disdain for SQL. For
a much more comprehensive discussion on difficulties with SQL, see these really
interesting notes by Jamie Brandon:
https://scattered-thoughts.net/writing/against-sql/.

* [Slides](https://bwlewis.github.io/duckdb_and_r/talk/talk.html)

Main overview:

* [Overview](https://bwlewis.github.io/duckdb_and_r/thoughts_on_duckdb.html)

The easy pieces:

* [Large out of core data](https://bwlewis.github.io/duckdb_and_r/taxi/taxi.html)
* [TPCH join-aggregate example from DuckDB](https://bwlewis.github.io/duckdb_and_r/tpch/tpch.html)
* [Last item per group](https://bwlewis.github.io/duckdb_and_r/last/last.html)
* [Genomic overlap joins](https://bwlewis.github.io/duckdb_and_r/ranges/ranges.html)
* ["As of" joins](https://bwlewis.github.io/duckdb_and_r/asof/asof.html)

A SQL rant born out of frustration while compiling these notes appears here:

[Declarative, Schmerative](https://bwlewis.github.io/duckdb_and_r/last/declarative.html)
