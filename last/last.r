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
                      company =  sample(make_believe), value = round(runif(N), 1), stringsAsFactors = TRUE)
example$date <- as.POSIXct(example$date) + example$value


# Task: obtain the last value by date per company group,

# One base R approach uses order and aggregate...it's a bit slow but stratighforward and the documentation
# for this approach is easy to follow.
t1 <- replicate(10, system.time({
  i <- order(example$company, example$date, method = "radix")
  base <<- aggregate(example$value[i], by = list(company = example$company[i]), FUN = function(x) tail(x, 1))
}))

# A vectorized way (faster than aggregate above)
t2 <- replicate(10, system.time({
  i <- order(example$company, example$date, method = "radix", decreasing = TRUE) # reverse order by company, date
  last <- c(1, diff(as.integer(example$company[i]))) != 0              # start of each company block (last value)
  vec  <<- data.frame(company = example$company[i[last]],
                      value   = example$value[i[last]])
}))

# One data table way, very fast but terse syntax
t3 <- replicate(10, system.time({
  example_dt <- data.table(example, key = c("company", "date"))
  dtb <<- example_dt[, tail(.SD, 1), by = c("company"), .SDcols = "value"]
}))


# A dplyr approach, very nice syntax and also really fast
t4 <- replicate(10, system.time({
  dpl <<- example %>% group_by(company) %>% summarize(value = last(value, order_by = date))
}))

con <- dbConnect(duckdb())
duckdb_register(con, "example", example)


# hmmmm. I tried to make sense of SQL window functions and came up with this:
# dbGetQuery(con, "SELECT last_value(value) OVER (PARTITION BY company ORDER BY date ASC) FROM example")
# but it did not work (wrong answer, took forever)
# this kind of query is where SQL really sucks for me. Simple web search for "sql last value per group"
# returns dozens of stack overflow and similar discussions with dozens of different approaches, often
# customized for particular database engines. So much for a standard query language, otherwise known
# as, you know, a grammar.


# dplyr to the rescue here, we can have dplyr show us a generated SQL query...
# Alas! I get an error:
#  tbl(con, "example") %>% group_by(company) %>% summarize(value = last(value, order_by = date))
#Error: `last()` is only available in a windowed (`mutate()`) context


# I finally figured this out but it's incredibly slow :/
duck1 <- dbGetQuery(con, "WITH ans AS (SELECT e.company, value FROM example AS e, (SELECT company, MAX(date) AS date FROM example GROUP BY company) AS m WHERE e.company = m.company AND e.date = m.date) SELECT company, MAX(value) FROM ans GROUP by company")
# Oh yeah, and the answers are wrong. Arrggghhhhh.

# SQLite gets the right answers with the same query but slowly.
library(RSQLite)
lite <- dbConnect(RSQLite::SQLite(), ":memory:")
dbWriteTable(lite, "example", example)
t6 <- replicate(10, system.time({
  dlite <<- dbGetQuery(lite, "WITH ans AS (SELECT e.company, value FROM example AS e, (SELECT company, MAX(date) AS date FROM example GROUP BY company) AS m WHERE e.company = m.company AND e.date = m.date) SELECT company, MAX(value) FROM ans GROUP by company")
}))
dbDisconnect(lite)

# Back to Duck DB:
# The problem is DuckDB does not understand R's extended POSIXct type. Silently mis-understanding type is probably a bug?
# The following gets the right results by converting the dates above to numeric.
# BUT It's incredibly slow, so much for magical SQL query optimization I guess.
duckdb_unregister(con, "example")
example$date_numeric <- as.numeric(example$date)
duckdb_register(con, "example", example)
t5 <- replicate(10, system.time({
  duck <<- dbGetQuery(con, "WITH ans AS (SELECT e.company, value FROM example AS e, (SELECT company, MAX(date_numeric) AS date FROM example GROUP BY company) AS m WHERE e.company = m.company AND e.date_numeric = m.date) SELECT company, MAX(value) FROM ans GROUP by company")
}))


dbDisconnect(con)

timings <- rbind(data.frame(approach = "Base R aggregate", elapsed = t1[3, ]),
                 data.frame(approach = "Base R vectorized", elapsed = t2[3, ]),
                 data.frame(approach = "data.table", elapsed = t3[3, ]),
                 data.frame(approach = "dplyr", elapsed = t4[3, ]),
                 data.frame(approach = "Duck DB", elapsed = t5[3, ]),
                 data.frame(approach = "SQLite", elapsed = t6[3, ]))
jpeg(file="last_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 3, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()

