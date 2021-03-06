# My thoughts on DuckDB and R with examples

R packages discussed in these notes include duckdb (of course), dplyr,
data.table, fst, xts, RSQLite, and vroom and a little Python Pandas by way of
reticulate.

The notes exhibit a mild disdain for SQL. For a much more comprehensive
discussion on difficulties with SQL, see these really interesting notes by
Jamie Brandon: https://scattered-thoughts.net/writing/against-sql/.
As an alternative to SQL I generally prefer dplyr.

These notes present several interesting, if somewhat eclectic, data-sciency examples. For more comprehensive and straight-up database-style performance comparisons, see the excellent work by H20 here:  https://h2oai.github.io/db-benchmark/ (where both R's data.table and DuckDB perform very well in general).

Also, you should check out <a href="https://github.com/pola-rs/polars">https://github.com/pola-rs/polars</a> for a remarkably high-performance new data frame implementation in Rust and geared to Python right now. This is the first data frame-like environment I have seen that really gives R's data.table competition, aside from KDB+ of course.


* [Slides](https://bwlewis.github.io/duckdb_and_r/talk/talk.html)

Main overview:

* [Overview](https://bwlewis.github.io/duckdb_and_r/thoughts_on_duckdb.html)

The easy pieces:

* [Large out of core data](https://bwlewis.github.io/duckdb_and_r/taxi/taxi.html)
* [TPCH join-aggregate example from DuckDB](https://bwlewis.github.io/duckdb_and_r/tpch/tpch.html)
* [Last item per group](https://bwlewis.github.io/duckdb_and_r/last/last.html)
* [Genomic overlap joins](https://bwlewis.github.io/duckdb_and_r/ranges/ranges_redux.html)
* ["As of" joins](https://bwlewis.github.io/duckdb_and_r/asof/asof.html)

A SQL rant born out of frustration while compiling these notes appears here:

[Declarative, Schmerative](https://bwlewis.github.io/duckdb_and_r/last/declarative.html)
