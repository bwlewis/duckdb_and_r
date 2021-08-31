cols <- c(
"c_customer_sk",
"c_customer_id",
"c_current_cdemo_sk",
"c_current_hdemo_sk",
"c_current_addr_sk",
"c_first_shipto_date_sk",
"c_first_sales_date_sk",
"c_salutation",
"c_first_name",
"c_last_name",
"c_preferred_cust_flag",
"c_birth_day",
"c_birth_month",
"c_birth_year",
"c_birth_country",
"c_login",
"c_email_address",
"c_last_review_date_sk")

library(data.table)
setDTthreads(8)
DSDGEN_PATH = "./dsdgen"  # TPC dsdgen program
SF = 300                  # scale factor

# generate data
system(sprintf("rm -f /tmp/customer.dat;%s -scale %d -dir /tmp -table customer -terminate N", DSDGEN_PATH, SF))

# load data
load300 <- system.time({
  customer <- fread("/tmp/customer.dat")
  names(customer) <- cols
})

# integer sort
dt_int_sort300 <- replicate(10, {
  x <- copy(customer)
  system.time(setkeyv(x, c("c_birth_year", "c_birth_month", "c_birth_day")))
})

# character sort
dt_char_sort300 <- replicate(10, {
  x <- copy(customer)
  system.time(setkeyv(x, c("c_first_name", "c_last_name")))
})


# DuckDB
library(duckdb)
con <- dbConnect(duckdb())
duckdb_register(con, "customer", customer)
dbExecute(con, "PRAGMA threads=8;")

duck_int_sort300 <- replicate(10, {
  system.time({dbGetQuery(con, "SELECT * FROM customer ORDER BY (c_birth_year, c_birth_month, c_birth_day)")})
})

duck_char_sort300 <- replicate(10, {
  system.time({dbGetQuery(con, "SELECT * FROM customer ORDER BY (c_first_name, c_last_name)")})
})


# ---------------------------------------------------------------------------------------------------
timings <- rbind(data.frame(approach = "DuckDB (int)", elapsed = duck_int_sort300[3, ]),
                 data.frame(approach = "DuckDB (char)", elapsed = duck_char_sort300[3, ]),
                 data.frame(approach = "data.table (int)", elapsed = dt_int_sort300[3, ]),
                 data.frame(approach = "data.table (char)", elapsed = dt_char_sort300[3, ]))
jpeg(file="tpcds_customer_upshot.jpg", quality=100, width=1000)
boxplot(elapsed ~ approach, data = timings, main = "Elapsed time (seconds), mean values shown below")
m = aggregate(list(mean=timings$elapsed), by=list(timings$approach), FUN=mean)
text(seq(NROW(m)), y = 5, labels = sprintf("%.2f", m$mean), cex = 1.5)
dev.off()

