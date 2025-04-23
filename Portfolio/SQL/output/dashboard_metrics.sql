-- BI-ready view with all calculated metrics
CREATE OR REPLACE VIEW vw_product_dashboard AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  pd.metric_date,
  pd.gross_revenue,
  pd.avg_7d_revenue,
  ROUND(pd.return_rate * 100, 2) AS return_pct,
  pd.inventory_days,
  -- Performance indicators
  CASE
    WHEN pd.return_rate > 0.1 THEN 'High Returns'
    WHEN pd.inventory_days > 60 THEN 'Overstocked'
    WHEN pd.inventory_days < 7 THEN 'Low Stock'
    ELSE 'Normal'
  END AS status_flag,
  -- Week-over-week comparison
  pd.gross_revenue / NULLIF(
    LAG(pd.gross_revenue, 7) OVER (
      PARTITION BY pd.product_id 
      ORDER BY pd.metric_date
    ), 0
  ) - 1 AS wow_growth
FROM product_performance_daily pd
JOIN products p ON pd.product_id = p.product_id;
