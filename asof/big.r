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

# Python/Pandas using R objects
library(reticulate)
pandas <- import("pandas")
t.py <- replicate(10, system.time({
  ans.py <<- pandas$merge_asof(calendar, data, on = "date")
}))
ans.py

# Native Python/Pandas
write.csv(calendar, file  ="calendar.csv", row.names = FALSE)
write.csv(data, file  ="data.csv", row.names = FALSE)
library(reticulate)
program <- '
import pandas as p
import numpy as np
import time

calendar = p.read_csv("calendar.csv")
calendar["date"] = p.to_datetime(calendar["date"])
data = p.read_csv("data.csv")
data["date"] = p.to_datetime(data["date"])

def run(i):
  tic = time.perf_counter()
  ans = p.merge_asof(calendar, data, on = "date")
  return time.perf_counter() - tic

ans = list(map(run, np.arange(1, 11)))
'
t.nativepy <- py_to_r(py_run_string(program))$ans


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

Q2 <- "
SELECT calendar.date as cal_date_col, arg_max(data.value, data.date) as latest_value
FROM calendar
LEFT JOIN data
ON calendar.date >= data.date
GROUP BY calendar.date"

print("Duck2")
t.duck2 <- replicate(6, system.time({
  ans.duck2 <<- dbGetQuery(con, Q2)
}))

print("RSQLite")
library(RSQLite)
lite <- dbConnect(RSQLite::SQLite(), ":memory:")
dbWriteTable(lite, "calendar", calendar)
dbWriteTable(lite, "data", data)
t.lite <- replicate(6, system.time({
  ans.lite <<- dbGetQuery(lite, Q)
}))

timings <- rbind(data.frame(approach = "zoo", elapsed = t.zoo[3, ]),
                 data.frame(approach = "xts", elapsed = t.xts[3, ]),
                 data.frame(approach = "data.table", elapsed = t.dt[3, ]),
                 data.frame(approach = "Pandas (R)", elapsed = t.py[3, ]),
                 data.frame(approach = "Pandas (Native)", elapsed = t.nativepy),
                 data.frame(approach = "SQL/Duck DB", elapsed = t.duck[3, ]),
                 data.frame(approach = "SQL/Duck DB v2", elapsed = t.duck2[3, ]),
                 data.frame(approach = "SQLite", elapsed = t.lite[3, ]))
jpeg(file="asof_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 40, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()

