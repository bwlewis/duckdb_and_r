library(disk.frame)
setup_disk.frame()
options(future.globals.maxSize = Inf)

files <- dir(pattern = "csv$")
print(system.time({
  yellow <- csv_to_disk.frame(files)
}))
#   user  system elapsed 
#  1.838   1.722 339.144 

# try 2 took this long :/
#   user  system elapsed 
#  2.802   1.813 629.076 



# Try #1 ...
ans <- yellow %>%
  select(passenger_count, trip_distance) %>%
  mutate(distance = round(trip_distance)) %>%
  group_by(passenger_count, distance) %>%
  summarize(count = length(passenger_count)) %>%
  arrange(desc(count))

# Unfortunately whe I try this, I get:
#Warning message:
#In arrange.disk.frame(., desc(count)) :
#  `arrange.disk.frame` is now deprecated. Please use `chunk_arrange` instead. This is in preparation for a more powerful `arrange` that sorts the whole disk.frame

#Error: arrange() failed at implicit mutate() step. 
#* Problem with `mutate()` input `..1`.
#✖ Input `..1` must be a vector, not a function.
#ℹ Input `..1` is `count`.
#Run `rlang::last_error()` to see where the error occurred.


# This likely is a problem in disk.frame though, not dplyr since this works fine:
# ans <- head(yellow, 1e5) %>% select( ...


# Try updating everything and latest github version...
#remotes::install_github("xiaodaigh/disk.frame")

# load time
# try 2 took this long :/
#   user  system elapsed 
#  2.802   1.813 629.076 


# got same error

# Let's try getting rid of that offending arrange (it's just on small data anyway)
# This works!
t2 <- replicate(10, system.time({
  ans_df <<- yellow %>%
    select(passenger_count, trip_distance) %>%
    mutate(distance = round(trip_distance)) %>%
    group_by(passenger_count, distance) %>%
    summarize(count = length(passenger_count)) %>%
    as.data.frame   %>%  # forcing the computation and returning result
    arrange(desc(count))
}))

mean(t2[3,])
#[1] 44.9435

# So, quite a bit slower and more problematic than most of the other methods.
