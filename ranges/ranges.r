library(parallel)
library(data.table)

f <- function(file)
{
  cmd <- sprintf("zcat %s | sed '/^#/d;/IMPRECISE/d' |  cut -f 2,4,5", file)
  name <- gsub(".phase.*","", file)
  print(name)
  x <- read.table(pipe(cmd), stringsAsFactors = FALSE)
  names(x) <- c("start", "ref", "alt")

  # Process extra comma-separated alleles
  idx <- grepl(",", x$alt)
  s <- strsplit(x[idx, "alt"], ",")
  # replace these rows with 1st alternate allele
  x[idx, "alt"] <- vapply(s, function(z) z[1], "")
  # Add new rows corresponding to other listed alternates
  ref <- x[idx, 1:2]
  N   <- vapply(s, length, 1) # numbers of alternates by row
  alt <- Map(function(i)
    {
      j <- which(N == i)
      cbind(ref[j, ], alt = vapply(s[j], function(z) z[i], ""))
    }, seq(2, max(N)))
  rbindlist(c(list(x), alt))
}

files <- dir(pattern="*.vcf.gz")
print(system.time({
  variants <- mcMap(f, files)
}))
# name by chromosome number:
names(variants) <- gsub("^ALL.chr", "", gsub(".phase.*", "", names(variants)))


cmd <- "cat genes.tsv | cut -f 6,13,14,15 | grep 'chr[7,8]' | sed -e 's/chr7/7/;s/chr8/8/'"
p <- pipe( cmd, open = "r")
genes <- na.omit(read.table(p, stringsAsFactors = FALSE, header = FALSE, sep = "\t"))
close(p)
names(genes) <- c("gene", "chromosome", "start", "end")
genes <- genes[genes$start > 0 & genes$end > 0, ]


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



### Duck DB

library(duckdb)
con <- dbConnect(duckdb::duckdb(), dbdir=":memory:", read_only=FALSE)

variants[[1]]$chromosome <- 7
variants[[2]]$chromosome <- 8
variants = rbindlist(variants)

duckdb_register(con, "genes", genes)
duckdb_register(con, "variants", variants)

Q <- 'SELECT G.gene, COUNT(V.ref) AS "count"
FROM genes G JOIN variants V
ON G.chromosome = V.chromosome
AND (G.start <= (V.start + length(ref)) AND G.end >= V.start)
GROUP BY 1 ORDER BY 2 DESC'

t3 <- replicate(10, system.time({ans.DB <<- dbGetQuery(con, Q)}))


timings <- rbind(data.frame(approach = "data.table", elapsed = t1[3, ]),
                 data.frame(approach = "IRanges", elapsed = t2[3, ]),
                 data.frame(approach = "Duck DB", elapsed = t3[3, ]))
jpeg(file="ranges_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = max(m$mean)/2, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()

