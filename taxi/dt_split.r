library(data.table)
library(parallel)

options(mc.cores=3)
setDTthreads(4)

print(system.time({
ans = rbindlist(mcMap(function(x) {
  rds = gsub("csv$", "rds", x)
  saveRDS(fread(x, showProgress = FALSE), file = rds, compress = FALSE)
}, dir(pattern = "*.csv"))
)}))

#   user  system elapsed
# 91.541  18.990 117.808


t_dtsplit <- replicate(10, system.time({
  ans_dtsplit <<- rbindlist(mcMap(function(f) {
    readRDS(f)[, list(count=.N), by = list(passenger_count, distance=as.integer(trip_distance))]
  }, dir(pattern = "rds$")))[, list(count = sum(count)), by = list(passenger_count, distance)][order(count, decreasing = TRUE)]
}))


save(t_dtsplit, ans_dtsplit, file="t_dtsplit.rdata")


# Most of this time is I/O. Can we speed things up? fst? basic lz4 compression maybe?
