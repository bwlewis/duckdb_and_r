library(data.table)

n_rows   <- as.integer(1e8)
n_groups <- as.integer(1e8)
set.seed(1)

df <- data.table(
        g1 = rep(seq(floor(n_groups/2)), each = floor(2*n_rows/n_groups)) - 1,
        g2 = rep(c(0,1), floor(n_rows/2)),
        d  = sample(n_rows, n_rows, replace = TRUE)
     )
df <- df[sample(nrow(df)),]

library(duckdb)
con <- dbConnect(duckdb())
duckdb_register(con, "df", df)
system.time({ans1 = dbGetQuery(con, "SELECT SUM(d), COUNT(*) FROM df GROUP BY g1, g2 LIMIT 1")})
system.time({ans2 = df[, list(count=.N, sum=sum(d)), by=c("g1", "g2")]})



