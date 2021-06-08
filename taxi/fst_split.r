library(data.table)
library(fst)
library(parallel)
setDTthreads(2)
threads_fst(4)

# CSV parsing is pretty fast, even sequentially
# note both data.table and fst are multithreaded so running in a single process here
print(system.time({
  for(f in dir(pattern="*.csv")) {
    fst <- gsub("csv$", "fst", f)
    write_fst(fread(f, showProgress = FALSE), path = fst)
  }
}))
#   user  system elapsed
#143.129  15.846  85.050

# Now the aggregation part...
options(mc.cores=3)
t_fst <- replicate(10, system.time({
ans_fst <<- rbindlist(mcMap(function(path) {
  data.table(fst(path)[, c("passenger_count", "trip_distance")])[, list(count=.N), by = list(passenger_count, distance=as.integer(trip_distance))]
}, dir(pattern = "fst$")))[, list(count = sum(count)), by = list(passenger_count, distance)][order(count, decreasing = TRUE)]
}))

save(t_fst, ans_fst, file="t_fst.rdata")
