set.seed(1)
end <- as.POSIXct("2020-06-20")
start <- as.POSIXct("2020-1-1")
dt <- as.integer(difftime(end, start, units = "secs"))
# Every minute
calendar <- data.frame(date = seq(from = start, to = end, by = "+1 min"))

N <- 5e6
data <- data.frame(date = end - runif(N) * dt, value = runif(N))
data <- data[order(data[["date"]]),]


library(zoo)
t.zoo <- replicate(6, system.time({
 ans.zoo <<- merge(calendar, na.locf(merge(calendar, data, all = TRUE)), all.x = TRUE)
}))

# Much (much) faster variation using xts
library(xts)
calendar.xts <- xts(, order.by = calendar[["date"]])
data.xts <- xts(data[["value"]], order.by = data[["date"]])
t.xts <- replicate(10, system.time({
  ans.xts <<- merge(calendar.xts, na.locf(merge(calendar.xts, data.xts)), join = "left")
}))

library(data.table)
setDTthreads(8)
data.dt <- data.table(data)
t.dt <- replicate(10, system.time({
  ans.dt <<- data.dt[calendar, on = "date", roll = TRUE]
}))

# Python/Pandas 
library(reticulate)
pandas <- import("pandas", convert = FALSE)
calendar_py <- r_to_py(calendar)
data_py <- r_to_py(data)
ans.py <- pandas$merge_asof(calendar_py, data_py, on = "date")
t.py <- replicate(10, system.time({
  invisible(pandas$merge_asof(calendar_py, data_py, on = "date"))
}))
ans.py <- py_to_r(ans.py)


library(duckdb)
Sys.setenv(DUCKDB_NO_THREADS = 8)
con <- dbConnect(duckdb())
duckdb_register(con, "data", data)
duckdb_register(con, "calendar", calendar)

Q <- "WITH z AS (
  SELECT date, (NULL) AS value FROM calendar
  UNION
  SELECT date, value FROM data
  ORDER BY date
),
a AS (
  SELECT date, value, ROW_NUMBER() OVER (
    ORDER BY date
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) * (CASE WHEN value IS NULL THEN 0 ELSE 1 END) AS i
  FROM z
),
b AS (
  SELECT date,  MAX(i) OVER (
    ORDER BY date
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS j
  FROM a
),
c AS (
  SELECT b.date, value FROM a, b
  WHERE a.i > 0 AND a.i = b.j
),
d AS (
  SELECT calendar.date, value FROM calendar, c
  WHERE calendar.date = c.date
  ORDER BY c.date
)
SELECT * FROM d UNION SELECT * FROM d ORDER BY date
"

print("Duck")
t.duck <- replicate(6, system.time({
  ans.duck <<- dbGetQuery(con, Q)
}))


print("RSQLite")
library(RSQLite)
lite <- dbConnect(RSQLite::SQLite(), ":memory:")
dbWriteTable(lite, "calendar", calendar)
dbWriteTable(lite, "data", data)
t.lite <- replicate(6, system.time({
  ans.lite <<- dbGetQuery(lite, Q)
}))


# Polars
# The following does not work, it mis-coverts the Pandas datetime[ns] values
# to datetime[ms]: (see https://github.com/pola-rs/polars/issues/476)
# p <- import("polars", convert = FALSE)
# calendar_plr <- p$convert$from_pandas(calendar_py)
# data_plr <- p$convert$from_pandas(data_py)
#
# (date time conversion should not be this hard)
#
# Instead we convert dates to int64 and back in polars:
program <- '
import numpy as np
import time
import polars as p
r.calendar_py["date"] = r.calendar_py["date"].astype(int)
r.data_py["date"] = r.data_py["date"].astype(int)
calendar_plr = p.convert.from_pandas(r.calendar_py)
data_plr = p.convert.from_pandas(r.data_py)
calendar_plr["date"] = calendar_plr["date"].cast(p.Datetime).dt.and_time_unit("ns")
data_plr["date"] = data_plr["date"].cast(p.Datetime).dt.and_time_unit("ns")

ans_plr = calendar_plr.join_asof(data_plr, on="date")

def run(i):
  tic = time.perf_counter()
  ans = ans_plr = calendar_plr.join_asof(data_plr, on="date")
  return time.perf_counter() - tic

ans = list(map(run, np.arange(1, 11)))
'
t.polars <- py_to_r(py_run_string(program))$ans


timings <- rbind(data.frame(approach = "zoo", elapsed = t.zoo[3, ]),
                 data.frame(approach = "xts", elapsed = t.xts[3, ]),
                 data.frame(approach = "data.table", elapsed = t.dt[3, ]),
                 data.frame(approach = "Pandas (R)", elapsed = t.py[3, ]),
                 data.frame(approach = "SQL/Duck DB", elapsed = t.duck[3, ]),
                 data.frame(approach = "SQLite", elapsed = t.lite[3, ]),
                 data.frame(approach = "Polars", elapsed = t.polars)
)
jpeg(file="asof_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 5, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()

