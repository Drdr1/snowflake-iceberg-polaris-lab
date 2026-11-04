-- Same queries as snowflake/60_cost_compare.sql, for Spark (or Glue Spark).
-- Run: spark-sql ... -f spark/benchmark.sql   (time each statement)
SELECT region, date_trunc('month', order_date) AS m, count(*) AS n, sum(amount) AS revenue
FROM polaris.lab.orders GROUP BY 1, 2 ORDER BY 1, 2;

SELECT customer_id, sum(amount) AS spend
FROM polaris.lab.orders
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY 1 ORDER BY spend DESC LIMIT 20;
