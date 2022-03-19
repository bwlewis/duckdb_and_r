set.seed(1)
end <- as.POSIXct("2020-06-20")
start <- as.POSIXct("2020-1-1")
dt <- as.integer(difftime(end, start, units = "secs"))
# Every minute
calendar <- data.frame(date = seq(from = start, to = end, by = "+1 min"))

N <- 5e6
data <- data.frame(date = end - runif(N) * dt, value = runif(N))
data <- data[order(data[["date"]]),]
 
library(data.table)
setDTthreads(8)
data.dt <- data.table(data)
ans.dt <- data.dt[calendar, on = "date", roll = TRUE]
t.dt <- replicate(10, system.time({
  invisible(data.dt[calendar, on = "date", roll = TRUE])
}))

# xts
library(xts)
calendar.xts <- xts(, order.by = calendar[["date"]])
data.xts <- xts(data[["value"]], order.by = data[["date"]])
ans.xts <- merge(calendar.xts, na.locf(merge(calendar.xts, data.xts)), join = "left")
t.xts <- replicate(10, system.time({
  invisible(merge(calendar.xts, na.locf(merge(calendar.xts, data.xts)), join = "left"))
}))

# Pandas
library(reticulate)
pandas <- import("pandas", convert = FALSE)
calendar_py <- r_to_py(calendar)
data_py <- r_to_py(data)
ans.py <- pandas$merge_asof(calendar_py, data_py, on = "date")
t.py <- replicate(10, system.time({
  invisible(pandas$merge_asof(calendar_py, data_py, on = "date"))
}))
ans.py <- py_to_r(ans.py)

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
ans.plr <- py_to_r(py_run_string("x=ans_plr.to_arrow().to_pandas()"))$x

# check equivalence (they all match)
# dates
#as.matrix(dist(rbind(xts=.index(ans.xts), dt=as.numeric(ans.dt$date), pandas=as.numeric(ans.py$date), polars=as.numeric(ans.plr$date))))
# values
#as.matrix(dist(rbind(xts=as.vector(ans.xts)[-1], dt=ans.dt$value[-1], pandas=ans.py$value[-1], polars=ans.plr$value[-1])))


