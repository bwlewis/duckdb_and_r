import time
import statistics
import tempfile
import os

import pandas as pd
import numpy as np

test_polars = False
test_arrow = False
test_duckdb = True
test_pandas = False

def timeit(f):
	times = []
	for n in range(1,3):
		start = time.time()
		f()
		times.append(time.time() - start)

	return(statistics.median(times))

def experiment(n_rows, n_groups):
	g1 = np.repeat(np.arange(0, int(n_groups/2)), int(n_rows/(n_groups/2)))
	g2 = np.tile(np.arange(0, 2), int(n_rows/2))
	d = np.random.randint(0, n_rows, n_rows)
	df = pd.DataFrame.from_dict({'g1' : g1, 'g2' : g2, 'd' : d})
	df = df.reindex(np.random.permutation(df.index))
	assert(len(df[['g1', 'g2']].drop_duplicates()) == n_groups)

	if test_pandas:
		if test_pandas:
			time_pandas = timeit(lambda:df.groupby(['g1', g2]).agg(
			    s=('d', 'sum'), ct=('d', 'count')).head(1))
			print("%d\t%d\tpandas\t%0.3f" % (n_rows, n_groups, time_pandas))

	if test_polars:
		import polars as pl
		df_polars = pl.DataFrame(df)
		time_polars = timeit(lambda:df_polars.groupby(['g1', 'g2']).agg(
		        [
		            pl.sum('d'),
		            pl.count(),
		        ]
		    ).head(1))
		print("%d\t%d\tpolars\t%0.3f" % (n_rows, n_groups, time_polars))

	if test_arrow:
		import pyarrow as pa
		df_arrow = pa.Table.from_pandas(df)
		time_arrow = timeit(lambda:df_arrow.group_by(['g1', 'g2']).aggregate([("d", "sum"), ("d", "count")]).take([1]))
		print("%d\t%d\tarrow\t%0.3f" % (n_rows, n_groups, time_arrow))
		
	if test_duckdb:
		import duckdb as dd
		con = dd.connect()

		time_duckdb = timeit(lambda:con.from_df(df
			).aggregate('sum(d), count(*)', 'g1, g2').limit(1).execute())
		print("%d\t%d\tduckdb\t%0.3f" % (n_rows, n_groups, time_duckdb))

for n_rows in 1000000, 10000000, 100000000:
	for n_groups in 1000, n_rows/1000, n_rows/100, n_rows/10 ,n_rows:
		experiment(n_rows, n_groups)





