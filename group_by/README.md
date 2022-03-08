# Notes on https://duckdb.org/2022/03/07/aggregate-hashtable.html

I can replicate the results of the article in Python (3.7) but not in R.
Running on an AWS hpc6a.48xlarge (96 x AMD EPYC 7R13 Processor cores, 384GiB
RAM). The original Python script from

https://gist.github.com/hannes/9b0e47625290b8af78de88e1d26441c0

produces:

```
1000000	1000	duckdb	0.059
1000000	1000	duckdb	0.039
1000000	10000	duckdb	0.041
1000000	100000	duckdb	0.031
1000000	1000000	duckdb	0.043
10000000	1000	duckdb	0.104
10000000	10000	duckdb	0.063
10000000	100000	duckdb	0.085
10000000	1000000	duckdb	0.091
10000000	10000000	duckdb	0.115
100000000	1000	duckdb	0.166
100000000	100000	duckdb	0.242
100000000	1000000	duckdb	0.734
100000000	10000000	duckdb	1.213
100000000	100000000	duckdb	1.482
```

So, about 1.5 seconds -- consistent with the article. But the following R
version:
```r
library(duckdb)
con <- dbConnect(duckdb())
dbExecute(con, "SET threads TO 48;")
dbGetQuery(con, "SELECT current_setting('threads');")
run <- function(n_rows, n_groups) {
  set.seed(1)
  df <- data.frame(
          g1 = rep(seq(floor(n_groups/2)), each = floor(2*n_rows/n_groups)) - 1,
          g2 = rep(c(0,1), floor(n_rows/2)),
          d  = sample(n_rows, n_rows, replace = TRUE)
        )
  df <- df[sample(nrow(df)),]
  duckdb_register(con, "df", df)
  cat("n_rows: ", n_rows, "n_groups: ", n_groups, "\n")
  print(replicate(3, {system.time(dbGetQuery(con, "SELECT SUM(d), COUNT(*) FROM df GROUP BY g1, g2 LIMIT 1"))}))
}

for(n_rows in c(1000000, 10000000, 100000000)) {
  for(n_groups in c(1000, n_rows/1000, n_rows/100, n_rows/10 ,n_rows)) {
    run(n_rows, n_groups)
  }
}
```
takes much longer to run, about 24s or so for the largerst problem. Also note that I set the threads
to 48 which improved results somehow over the default of 96. (The system tested had 96 physical cores.)

???
