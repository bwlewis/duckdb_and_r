library(data.table)
library(parallel)
setDTthreads(8)
files = dir(pattern = "csv$")

# Try 2...using 50GB swap
yellow = data.table()
print(system.time({
  for(f in files) yellow <- rbind(yellow, fread(f), fill = TRUE)
}))
#     user   system  elapsed
#  140.540  252.163 1180.802    # much slower than duckdb


t_dtswap <- replicate(10, system.time({
  ans_dtswap <<- yellow[, list(count = .N), by = list(passenger_count, distance=as.integer(trip_distance))][order(count, decreasing = TRUE)]
}))

save(t_dtswap, ans_dtswap, file="t_dtswap.rdata")
