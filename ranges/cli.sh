#!/bin/bash
DUCK=./duckdb    # CLI assumed to exist on this path

for x in 1 2 3 4 5 6 7 8 9 10; do
cat << EOF | ${DUCK} -csv | tail -n 1
CREATE TABLE variants AS SELECT * FROM 'variants.csv';
CREATE TABLE genes AS SELECT * FROM 'genes.csv';
CREATE TABLE t0 AS SELECT CURRENT_TIMESTAMP;
SELECT G.gene, COUNT(V.ref) AS 'count'
FROM genes G JOIN variants V
ON G.chromosome = V.chromosome
AND (G.start <= (V.start + length(ref)) AND G.end >= V.start)
GROUP BY 1 ORDER BY 2 DESC;
CREATE TABLE t1 AS SELECT CURRENT_TIMESTAMP;
SELECT date_part('microseconds', t1 - t0) FROM (SELECT "main.current_timestamp()" AS t0 FROM t0) CROSS JOIN (SELECT "main.current_timestamp()" AS t1 FROM t1);
EOF
done
