library(parallel)
library(data.table)

load("variants_genes.rdata")

cat("Data table\n", file=stderr())
library(data.table)
overlap <- function(chromosome)
{
  x <- variants[[as.character(chromosome)]]
  x$end <- x$start + nchar(x$ref)
  g <- genes[genes$chromosome == chromosome, c(1, 3, 4)]
  setDT(g)
  setkey(g, start, end)
  setDT(x)
  foverlaps(x, g, nomatch=0)[, .N, by = gene]
}
t1 <- replicate(10, system.time(ans.DT <<- rbindlist(mcMap(overlap, c(7, 8)))))
ans.DT <- ans.DT[order(ans.DT[["N"]], decreasing = TRUE), ]


cat("IRanges\n", file=stderr())
# IRanges
library(IRanges)
overlap <- function(chromosome)
{
  x <- variants[[as.character(chromosome)]]
  ir1 <- IRanges(start = x$start, end = x$start + nchar(x$ref))
  g <- genes[genes$chromosome == chromosome, c(1, 3, 4)]
  ir2 <- IRanges(start = g$start, end = g$end)
  ans <- findOverlaps(ir1, ir2)
  data.frame(gene = g$gene, count = countRnodeHits(ans))
}
# Compute the overlaps in parallel for chromosomes 7 and 8
t2 <- replicate(10, system.time({ans.IR <<- rbindlist(mcMap(overlap, 7:8))}))
ans.IR <- ans.IR[order(ans.IR[["count"]], decreasing = TRUE), ]



### Duck DB (R package)
cat("DuckDB R\n", file=stderr())
library(duckdb)
con <- dbConnect(duckdb::duckdb(), dbdir=":memory:", read_only=FALSE)

variants[[1]]$chromosome <- 7
variants[[2]]$chromosome <- 8
variants = rbindlist(variants)

duckdb_register(con, "genes", genes)
duckdb_register(con, "variants", variants)

Q <- 'PRAGMA threads=8;
SELECT G.gene, COUNT(V.ref) AS "count"
FROM genes G JOIN variants V
ON G.chromosome = V.chromosome
AND (G.start <= (V.start + length(ref)) AND G.end >= V.start)
GROUP BY 1 ORDER BY 2 DESC'

t3 <- replicate(10, system.time({ans.DB <<- dbGetQuery(con, Q)}))


### DuckDB (CLI)
cat("DuckDB CLI\n", file=stderr())
# The following example assumes that a current `duckdb` command-line program
# is available in the current directory. See the cli.sh script for details.
# First we export variants and genes as CSV files for DuckDB.

fwrite(variants, file="variants.csv")
fwrite(genes, file="genes.csv")
t4 <- as.numeric(system("./cli.sh", intern=TRUE)) / 1e6




timings <- rbind(data.frame(approach = "data.table", elapsed = t1[3, ]),
                 data.frame(approach = "IRanges", elapsed = t2[3, ]),
                 data.frame(approach = "Duck DB (R)", elapsed = t3[3, ]),
                 data.frame(approach = "Duck DB (CLI)", elapsed = t4))
jpeg(file="ranges_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = max(m$mean)/2, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()


