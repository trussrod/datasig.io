# 🏁 **Product Performance Analytics Dashboard** 📊

## 📝 **Project Description:**

This project demonstrates a **production-grade SQL ETL pipeline** that transforms raw e-commerce data into actionable product performance metrics. The solution powers executive dashboards with daily insights on revenue trends, inventory health, and product returns, serving as the foundation for data-driven decision making.

## 🔧 **Topics Covered:**

- **Data Modeling**:
  - Designed a star schema with fact (orders, order_items) and dimension (products) tables
  - Implemented referential integrity with primary/foreign keys
  - Added constraints for data quality (CHECK, NOT NULL)

- **ETL Development**:
  - Built daily aggregation of product metrics at the lowest grain (product/day)
  - Created incremental loading pattern for efficient pipeline runs
  - Developed slowly changing dimension (SCD) logic for product attributes

- **Advanced Analytics**:
  - **Window Functions**: 7-day moving averages using ROWS BETWEEN frames
  - **Time Intelligence**: YoY comparisons with LAG(365) pattern
  - **Business Metrics**: Inventory turnover, return rates, and revenue trends

## 📈 **Key Features**:
- **Optimized SQL**: Indexed columns for 100M+ record performance
- **Dashboard-Ready**: Clean output structure for visualization tools
- **Temporal Analysis**: Multiple time-based comparisons (DOD, WOW, YOY)
- **Data Quality**: NULL handling and divide-by-zero protection

## 🧩 **Skills Demonstrated**:
- **SQL Expertise**: Complex CTEs, window functions, and query optimization
- **Dimensional Modeling**: Proper star schema implementation
- **Business Translation**: Technical metrics mapped to executive KPIs
- **Pipeline Design**: Daily refresh pattern with incremental processing

## 📁 **Project Structure**:
1. **`schema/`** - Database design
   - `01_fact_tables.sql`: Transactional data models (orders, order items)
   - `02_dimensions.sql`: Reference/master data (products catalog)

2. **`etl/`** - Data processing
   - `daily_aggregation.sql`: Main transformation logic
   - `refresh_materialized.sql`: Performance optimization scripts

3. **`output/`** - Analysis ready datasets
   - `dashboard_metrics.sql`: Final views for visualization
   - `sample_output.csv`: Example data for documentation


## ▶️ **Implementation Example**:
<pre>```
-- Daily product metrics calculation
INSERT INTO product_performance_daily
WITH daily_sales AS (
  SELECT
    product_id,
    DATE_TRUNC('day', order_date) AS metric_date,
    SUM(quantity * price) AS gross_revenue,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(CASE WHEN returned THEN quantity ELSE 0 END) AS returned_units
  FROM orders
  JOIN order_items USING (order_id)
  WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
  GROUP BY 1, 2
)
SELECT 
  ds.*,
  -- 7-day moving average
  AVG(gross_revenue) OVER (
    PARTITION BY product_id 
    ORDER BY metric_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS avg_7d_revenue,
  -- Inventory days calculation
  p.current_inventory / NULLIF(
    AVG(quantity) OVER (
      PARTITION BY product_id
      ORDER BY metric_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0
  ) AS inventory_days
FROM daily_sales ds
JOIN products p USING (product_id); ```</pre>

## 📁 **Results**:
- 🔗 [Full SQL Script](etl_pipeline.sql)
