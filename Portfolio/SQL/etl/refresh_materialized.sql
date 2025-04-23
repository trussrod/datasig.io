-- Optimized refresh for dashboard views
CREATE OR REPLACE VIEW product_performance_7d AS
SELECT 
  p.product_id,
  p.product_name,
  p.category,
  pd.metric_date,
  pd.gross_revenue,
  pd.avg_7d_revenue,
  ROUND(pd.return_rate * 100, 2) AS return_pct,
  pd.inventory_days,
  -- Calculate YoY growth
  (pd.gross_revenue / NULLIF(
    LAG(pd.gross_revenue, 365) OVER (
      PARTITION BY pd.product_id 
      ORDER BY pd.metric_date
    ), 0
  ) - 1 AS yoy_growth
FROM product_performance_daily pd
JOIN products p ON pd.product_id = p.product_id
WHERE pd.metric_date >= CURRENT_DATE - INTERVAL 7 DAY;
