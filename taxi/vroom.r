library(data.table)
library(vroom)
library(parallel)

# The vroom approach lazily parses. It's great for one-off work involving a
# small subset of a huge text file (like this problem).

# alas, this crashes out of memory on my 8GB machine:
# x <- vroom::vroom(dir(pattern = "csv$"))

# so, let's split
options(mc.cores = 2)  # more than this hits memory OOM problems for me
types <- "iTTidddicddiddddddd"
t_vroom <- replicate(5, system.time({
ans_vroom <<- rbindlist(Map(function(x) {
  f <- vroom(x, col_names = TRUE, col_types = types)
  data.table(f[, c("passenger_count", "trip_distance")])[, list(count=.N), by = list(passenger_count, distance=as.integer(trip_distance))]
}, dir(pattern = "*.csv"))
)[, list(count = sum(count)), by = list(passenger_count, distance)][order(count, decreasing = TRUE)]}))

save(t_vroom, ans_vroom, file="t_vroom.rdata")
