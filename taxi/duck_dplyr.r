library(duckdb)
Sys.setenv(DUCKDB_NO_THREADS = 8)
con <- dbConnect(duckdb("./duck"))

library(dplyr)
t_duck_dplyr <- replicate(10, system.time({
  ans_duck_dplyr <<- tbl(con, "yellow") %>%
  select(passenger_count, trip_distance) %>%
  mutate(distance = as.integer(trip_distance)) %>%
  group_by(passenger_count, distance) %>%
  summarize(count = n()) %>%
  arrange(desc(count)) %>%
  collect
}))

save(t_duck_dplyr, ans_duck_dplyr, file="t_duck_dplyr.rdata")
