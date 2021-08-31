NTHREADS = 1
Sys.setenv(DUCKDB_NO_THREADS=NTHREADS)

library(duckdb)
con <- dbConnect(duckdb())

# Set up data
set.seed(1)
x <- data.frame(ints = sample(seq(100e6)))
duckdb_register(con, "x", x)

t_duck <- replicate(10, system.time({s_duck <<- dbGetQuery(con, "SELECT * FROM x ORDER BY ints")}))

t_base <- replicate(10, system.time({s <<- x[order(x[["ints"]]), ]}))

library(dplyr)
t_dpl <- replicate(10, system.time({s_dpl <<- x %>% arrange(ints)}))

library(data.table)
setDTthreads(NTHREADS)
t_dt <- replicate(10, system.time({s_dt <<- data.table(x, key="ints")}))


# -----------------------------------------------------------------------------------------------
timings <- rbind(data.frame(approach = "DuckDB", elapsed = t_duck[3, ]),
                 data.frame(approach = "base R", elapsed = t_base[3, ]),
                 data.frame(approach = "dplyr", elapsed = t_dpl[3, ]),
                 data.frame(approach = "data.table", elapsed = t_dt[3, ]))
jpeg(file="sort_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 6, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()
