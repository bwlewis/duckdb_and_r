library(duckdb)
Sys.setenv(DUCKDB_NO_THREADS = 8)
files = dir(pattern = "csv$")
con <- dbConnect(duckdb("./duck"))
print(system.time({
  dbExecute(con, "CREATE TABLE yellow(
VendorID INTEGER,
tpep_pickup_datetime TIMESTAMP,
tpep_dropoff_datetime TIMESTAMP,
passenger_count INTEGER,
trip_distance DOUBLE,
pickup_longitude DOUBLE,
pickup_latitude  DOUBLE,
RateCodeID INTEGER,
store_and_fwd_flag  VARCHAR(1),
dropoff_longitude DOUBLE,
dropoff_latitude DOUBLE,
payment_type INTEGER,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE);")
  for(f in files) {
    q = sprintf("COPY yellow FROM '%s' ( AUTO_DETECT TRUE );", f)
    dbExecute(con, q)
  }
}))

#   user  system elapsed
#257.369  34.990 459.670

q <- "
SELECT passenger_count,
  cast(trip_distance as int) AS distance,
  count(*) AS count
FROM yellow
GROUP BY passenger_count, distance
ORDER BY count desc"
t_duck <- replicate(10, system.time({
  ans_duck <<- dbGetQuery(con, q)
}))


save(t_duck, ans_duck, file="t_duck.rdata")
