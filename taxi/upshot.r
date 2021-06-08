load("t_dtsplit.rdata")
load("t_dtswap.rdata")
load("t_duck_dplyr.rdata")
load("t_duck.rdata")
load("t_fst.rdata")
load("t_vroom.rdata")

timings <- rbind(data.frame(approach = "DuckDB", elapsed = t_duck[3, ]),
                 data.frame(approach = "dplyr + DuckDB", elapsed = t_duck_dplyr[3, ]),
                 data.frame(approach = "data.table + swap", elapsed = t_dtswap[3, ]),
                 data.frame(approach = "data.table + MR", elapsed = t_dtsplit[3, ]),
                 data.frame(approach = "fst + data.table + MR", elapsed = t_fst[3, ]),
                 data.frame(approach = "vroom + MR", elapsed = t_vroom[3, ]))
jpeg(file="query_4.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed query time (seconds), mean values shown below", xlab = "approach (MR = map/reduce)")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 17, labels = sprintf("%.1f", m$mean), cex = 1.5)
dev.off()

jpeg(file="taxi_load.jpg", quality=100, width=1000)
load <- rbind(data.frame(approach = "DuckDB", elapsed = 459.7),
              data.frame(approach = "data.table + swap", elapsed = 1180.8),
              data.frame(approach = "data.table + MR", elapsed = 117.8),
              data.frame(approach = "fst + data.table + MR", elapsed = 85.1),
              data.frame(approach = "vroom + MR", elapsed = 0))
b = barplot(elapsed ~ approach, data = load, main = "Elapsed load time (seconds)", xlab = "approach (MR = map/reduce)")
text(b[,1][order(load[["approach"]])], y = 800, labels = sprintf("%.0f", load$elapsed), cex = 1.5)
dev.off()
