library(data.table)
setDTthreads(48)

run <- function(n_rows, n_groups) {
  set.seed(1)
  df <- data.table(
          g1 = rep(seq(floor(n_groups/2)), each = floor(2*n_rows/n_groups)) - 1,
          g2 = rep(c(0,1), floor(n_rows/2)),
          d  = sample(n_rows, n_rows, replace = TRUE)
        )
  df <- df[sample(nrow(df)),]
  cat("n_rows: ", n_rows, "n_groups: ", n_groups, "\n")
  print(replicate(3, {system.time(df[, list(count=.N, sum=sum(d)), by=c("g1", "g2")])}))
}

for(n_rows in c(1000000, 10000000, 100000000)) {
  for(n_groups in c(1000, n_rows/1000, n_rows/100, n_rows/10 ,n_rows)) {
    run(n_rows, n_groups)
  }
}
