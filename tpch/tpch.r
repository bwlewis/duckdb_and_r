NTHREADS = 8  # Set this appropriately for your system, my test laptop has 8 physical CPU cores.
Sys.setenv(DUCKDB_NO_THREADS=NTHREADS)
library(duckdb)
con <- dbConnect(duckdb())

# Load from parquet into DuckDB
dbExecute(con, "CREATE TABLE lineitem AS SELECT * FROM 'lineitemsf1.snappy.parquet'")
dbExecute(con, "CREATE TABLE orders AS SELECT * FROM 'orders.parquet'")

# NOTE not a date in the parquet file
# since l_shipdate is character-valued, a natural question arises about how SQL handles
# the WHERE l_shipdate <= DATE '1998-09-02'  expression. Is the comparison character or date based?
# Maybe it doesn't matter here, but  implicit type casting is left somewhat mysterious:
#   https://duckdb.org/docs/sql/expressions/cast
# In R's case, for instance:
#   "2020-11-1" < as.Date("2020-2-1")
# invokes a type promotion of as.Date("2020-11-1") and then runs the date comparison.

Q1 <- "SELECT l_returnflag,
       l_linestatus,
       sum(l_extendedprice),
       min(l_extendedprice),
       max(l_extendedprice),
       avg(l_extendedprice)
FROM lineitem lineitem
JOIN orders orders ON (l_orderkey=o_orderkey)
WHERE l_shipdate <= DATE '1998-09-02'
  AND o_orderstatus='O'
GROUP BY l_returnflag,
         l_linestatus"

t1 <- replicate(50, system.time({duck <<- dbGetQuery(con, Q1)}))


# Load DuchDB data into R
lineitem <- dbGetQuery(con, "SELECT * FROM lineitem")
orders <- dbGetQuery(con, "SELECT * FROM orders")
lineitem[["l_shipdate"]] <- as.Date(lineitem[["l_shipdate"]]) 


# A straight-up vectorized approach; the most natural base R way (for me, at least) to solve this problem.
t2 <- replicate(50, system.time({
  h <- orders[["o_orderstatus"]] == "O"
  i <- lineitem[["l_orderkey"]] %in%  orders[["o_orderkey"]][h]   &  lineitem[["l_shipdate"]] <= as.Date("1998-09-02")
  vec <<- Map(function(x) list(sum = sum(x, na.rm = TRUE),
                                min = min(x, na.rm = TRUE),
                                max = max(x, na.rm = TRUE),
                                avg = mean(x, na.rm = TRUE)),
               split(lineitem[["l_extendedprice"]][i], interaction(lineitem[["l_returnflag"]][i], lineitem[["l_linestatus"]][i])))
}))
# note floating point difference here
# vec[[1]][["sum"]] - duck[3]
# the R answer is slightly more accurate because of R internal use of quad precision in `sum`, which also imposes
# a performance penalty on R.
# Summation order can, of course, also affect the computed result in floating-point arithmetic.
# This base R vectorized approach works fine for tiny data like this, but will not scale because the use of
# split here induces a somewhat superfluous data copy. For large data that would not be good.

# The dplyr approach is much faster than merge/aggregate, but still a bit
# slower than base R and Duck DB.  BUT dplyr syntax is, in my opinion, superior
# to SQL; it's unambiguously imperative, easily composable, and much more
# flexible (much richer custom functions and output patterns are supported,
# easier to combine scalar results and vectors, etc.).
library(dplyr)
t3 <- replicate(50, system.time({
  o <- orders %>%
       select(o_orderkey, o_orderstatus)  %>%
       filter(o_orderstatus == "O")
  dpl <<- lineitem %>%
           select(l_orderkey, l_shipdate, l_returnflag, l_linestatus, l_extendedprice) %>%
           filter(l_shipdate <= as.Date("1998-09-02")) %>%
           inner_join(o, by = c("l_orderkey" = "o_orderkey")) %>%
           group_by(l_returnflag, l_linestatus) %>%
           summarize(sum = sum(l_extendedprice, na.rm = TRUE),
                     min = min(l_extendedprice, na.rm = TRUE),
                     max = max(l_extendedprice, na.rm = TRUE),
                     avg = mean(l_extendedprice, na.rm = TRUE), .groups = "keep")
}))

# Let's try using DuckDB with dplyr, nearly identical to dplyr only syntax,
# which is nice!  A bit slower than native SQL DuckDB, but you gain all the
# substantial benefits of dplyr over SQL. This is a good general purpose use
# case IMO for data external to R.
t4 <- replicate(50, system.time({
  o <- tbl(con, "orders") %>%
      select(o_orderkey, o_orderstatus)  %>%
      filter(o_orderstatus == "O")
  dplyr_duck <<- collect(tbl(con, "lineitem") %>%
             select(l_orderkey, l_shipdate, l_returnflag, l_linestatus, l_extendedprice) %>%
             filter(l_shipdate <= as.Date("1998-09-02")) %>%
             inner_join(o, by = c("l_orderkey" = "o_orderkey")) %>%
             group_by(l_returnflag, l_linestatus) %>%
             summarize(sum = sum(l_extendedprice, na.rm = TRUE),
                       min = min(l_extendedprice, na.rm = TRUE),
                       max = max(l_extendedprice, na.rm = TRUE),
                       avg = mean(l_extendedprice, na.rm = TRUE), .groups = "keep"))
}))

# Done with DuckDB
dbDisconnect(con, shutdown = TRUE)

# Data table
library(data.table)
setDTthreads(NTHREADS)
lineitem <- as.data.table(lineitem)
orders <- as.data.table(orders)

# This is fast, I wonder though is there a better DT way?
t5 <- replicate(50, system.time({
  dtbl <<- (lineitem[l_shipdate <= as.Date("1998-09-02"),
                  c("l_orderkey", "l_returnflag", "l_linestatus", "l_extendedprice")] [orders[o_orderstatus == "O", "o_orderkey"],
                    on = c("l_orderkey" = "o_orderkey"), nomatch = NULL])[, list(sum = sum(l_extendedprice, na.rm=TRUE),
                                                               min = min(l_extendedprice, na.rm=TRUE),
                                                               max = max(l_extendedprice, na.rm=TRUE),
                                                               avg = mean(l_extendedprice, na.rm=TRUE)), by=c("l_linestatus", "l_returnflag")]
}))


timings <- rbind(data.frame(approach = "DuckDB", elapsed = t1[3, ]),
                 data.frame(approach = "Base R", elapsed = t2[3, ]),
                 data.frame(approach = "dplyr", elapsed = t3[3, ]),
                 data.frame(approach = "dplyr + DuckDB", elapsed = t4[3, ]),
                 data.frame(approach = "data.table", elapsed = t5[3, ]))
jpeg(file="tpch_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 1.2, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()



# In the spirit of the database join/aggregate syntax. R's functions for this
# are verbose but very nicely, in my opinion, imperative and explicit.
#t9 <- replicate(10, system.time({
#  i <- lineitem[["l_shipdate"]] <= as.Date("1998-09-02")
#  j <- c("l_orderkey", "l_returnflag", "l_linestatus", "l_extendedprice")
#  h <- orders[["o_orderstatus"]] == "O"
#  k <- c("o_orderkey")
#  m <- merge(x = lineitem[i, c("l_orderkey", "l_returnflag", "l_linestatus", "l_extendedprice"), drop = FALSE],
#             y = orders[h, k, drop = FALSE], by.x = "l_orderkey", by.y = "o_orderkey")
#  fun <- function (x) c(sum = sum(x, na.rm = TRUE),
#                        min = min(x, na.rm = TRUE),
#                        max = max(x, na.rm = TRUE),
#                        avg = mean(x, na.rm = TRUE))
#  agg <<- aggregate(m$l_extendedprice, by = list(l_returnflag=m$l_returnflag, l_linestatus=m$l_linestatus),
#                  FUN = fun)
#}))
# A lot slower than duckdb! But, if you use data tables in the same expression instead
# of data frames, it magically runs much much faster.

