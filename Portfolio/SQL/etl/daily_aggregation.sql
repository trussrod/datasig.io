-- Daily product metrics ETL
INSERT INTO product_performance_daily
WITH daily_sales AS (
  SELECT
    oi.product_id,
    DATE(o.order_date) AS metric_date,
    SUM(oi.quantity * oi.price) AS gross_revenue,
    SUM(oi.quantity) AS total_units,
    SUM(CASE WHEN oi.returned THEN oi.quantity ELSE 0 END) AS returned_units
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  WHERE 
    o.order_date >= CURRENT_DATE - INTERVAL 8 DAY
    AND o.status = 'completed'
  GROUP BY 1, 2
),

with_rolling_metrics AS (
  SELECT
    product_id,
    metric_date,
    gross_revenue,
    AVG(gross_revenue) OVER (
      PARTITION BY product_id
      ORDER BY metric_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_7d_revenue,
    returned_units / NULLIF(total_units, 0) AS return_rate
  FROM daily_sales
)

SELECT
  w.metric_date,
  w.product_id,
  w.gross_revenue,
  w.avg_7d_revenue,
  w.return_rate,
  -- Calculate inventory days: current stock / avg daily sales rate
  p.current_inventory / NULLIF(w.avg_7d_revenue/p.price, 0) AS inventory_days
FROM with_rolling_metrics w
JOIN products p ON w.product_id = p.product_id
WHERE w.metric_date = CURRENT_DATE - INTERVAL 1 DAY
ON DUPLICATE KEY UPDATE
  gross_revenue = VALUES(gross_revenue),
  avg_7d_revenue = VALUES(avg_7d_revenue),
  return_rate = VALUES(return_rate),
  inventory_days = VALUES(inventory_days);
