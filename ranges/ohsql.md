# Oh, SQL. Why bother?

I find SQL incredibly frustrating to work with. The following completely
trivial example took me hours to resolve. At a high level, I simply wanted
to record some times and subtract them (should be very easy).

The following notes were entered interactively in a running DuckDB command
line interface shell (CLI), for instance by running:

```
./duckdb
```


## 1. Recording the times (easy, no problem).

```
CREATE TABLE x AS SELECT CURRENT_TIMESTAMP;

CREATE TABLE y AS SELECT CURRENT_TIMESTAMP;
```

The above step creates two tables with single column and row, the time that
they were created. In my example of course I was doing something else
in-between the two. Now I simply want to subtract the two time values.

## 2. Put the tables together (easy, no problem).

I first tried to put the two time values together in one table. The following
expression works easily:

```
SELECT * FROM x CROSS JOIN y;
┌─────────────────────────┬─────────────────────────┐
│   current_timestamp()   │   current_timestamp()   │
├─────────────────────────┼─────────────────────────┤
│ 2022-05-30 11:54:33.601 │ 2022-05-30 11:54:49.664 │
└─────────────────────────┴─────────────────────────┘
```

Note that the columns from each table have the same name and include
parentheses in their names. Maybe those could be problems? Let's
see:

## 3. Select one time value from the combined table, try 1.

Since there are two columns with the same name I expected this to
fail anyway:

```
SELECT  "current_timestamp()" from x CROSS JOIN y;
Error: Binder Error: Ambiguous reference to column name "current_timestamp()"
       (use: "y.current_timestamp()" or "x.current_timestamp()")
```

Nice! DuckDB gives me an informative error that shows me what to do...

## 4. ...but it does not work

```
SELECT  "x.current_timestamp()" from x CROSS JOIN y;
Error: Binder Error: Referenced column "x.current_timestamp()" not found in FROM clause!
Candidate bindings: "y.current_timestamp()", "x.current_timestamp()"
LINE 1: SELECT  "x.current_timestamp()" from x CROSS JO...
```

## 5. Rename columns to unique names I guess?

Sub-queries can be used to change each column name to something unique and more
acceptable, I guess. The following works:

```
SELECT t1 - t0 FROM (SELECT "current_timestamp()" AS t0 FROM x)
  CROSS JOIN (SELECT "current_timestamp()" AS t1 FROM y);
```

But of course, I should not have to do this. SQL makes life harder in so many
little ways.


## 5. And it broke again.

Apparently sometime between DuckDB 0.32 and DuckDB v0.3.5-dev651 a25b6e307
naming has changed. The old query does not work, but this one does:


```
SELECT t1 - t0 FROM (SELECT "main.current_timestamp()" AS t0 FROM x)
  CROSS JOIN (SELECT "main.current_timestamp()" AS t1 FROM y);
```

Another few minutes of life wasted on SQL.
