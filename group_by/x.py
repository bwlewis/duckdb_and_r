import time
import statistics
import tempfile
import os

import pandas as pd
import numpy as np


n_rows = 100000000
n_groups = n_rows
g1 = np.repeat(np.arange(0, int(n_groups/2)), int(n_rows/(n_groups/2)))
g2 = np.tile(np.arange(0, 2), int(n_rows/2))
d = np.random.randint(0, n_rows, n_rows)
df = pd.DataFrame.from_dict({'g1' : g1, 'g2' : g2, 'd' : d})
df = df.reindex(np.random.permutation(df.index))
assert(len(df[['g1', 'g2']].drop_duplicates()) == n_groups)
import duckdb as dd
con = dd.connect()
ans = con.from_df(df).aggregate('sum(d), count(*)', 'g1, g2').execute()
