# Examples of SQL implementation inconsistencies...
# postgres https://hakibenita.com/sql-group-by-first-last-value
# mysql https://stackoverflow.com/questions/1313120/retrieving-the-last-record-in-each-group-mysql#1313293

library(data.table)
library(duckdb)
library(dplyr)
source("fake.r")

set.seed(1)
N_GROUPS <- 100
MAX_LEN <- 1e5

make_believe <- list(lengths = sample(MAX_LEN, N_GROUPS, replace = TRUE),
                     values = paste(sample(names, N_GROUPS, replace = TRUE),
                                    sample(sectors, N_GROUPS, replace = TRUE)))
make_believe <- inverse.rle(structure(make_believe, class = "rle"))
N <- length(make_believe)

example <- data.frame(date = Sys.Date() - sample(365, N, replace = TRUE),
                      company =  sample(make_believe), value = runif(N), stringsAsFactors = TRUE)
example$date <- as.POSIXct(example$date) + runif(N)


example$date_numeric <- as.numeric(example$date)
con <- dbConnect(duckdb())
duckdb_register(con, "example", example)

# Note need to name result of interest 'last_value' to avoid a dumb name conflict with value. Way around this?
q2 = "SELECT company, MAX(last_value) FROM
(SELECT company,
LAST_VALUE (value) OVER (
  PARTITION BY company
  ORDER BY date_numeric
        RANGE BETWEEN UNBOUNDED PRECEDING AND 
        UNBOUNDED FOLLOWING
  ) last_value
FROM example) as superfluous_required_alias
GROUP BY company;"

t6 <- replicate(10, system.time({
  duck2 <<- dbGetQuery(con, q2)
}))

print(summary(t6[3,]))
