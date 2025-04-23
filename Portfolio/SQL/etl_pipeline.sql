/* 
================================================================================
THEORETICAL FOUNDATIONS:
This implementation demonstrates key analytics engineering concepts:
1. Dimensional Modeling: Star schema with fact (orders/order_items) and dimension (products) tables
2. ETL Best Practices: Incremental loading, idempotent operations, data quality checks
3. Time-Series Analytics: Window functions for moving averages and period-over-period comparisons
4. Business Intelligence: Transformation of raw data into actionable KPIs
================================================================================
*/

-- =============================================
-- SECTION 1: DATABASE SCHEMA SETUP
-- =============================================

/*
SCHEMA DESIGN PRINCIPLES:
- Star Schema: Optimized for analytics with clear fact/dimension separation
- Referential Integrity: Foreign keys enforce data relationships
- Constraints: Ensure data quality at database level
- Indexing: Careful index selection for query performance
*/
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS product_performance_daily;

-- Dimension table containing product master data
-- Slowly Changing Dimension (SCD) pattern with created_at/updated_at timestamps
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,  -- Dimension attribute for slicing
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),  -- Positive price constraint
    cost DECIMAL(10,2) COMMENT 'Manufacturing cost for margin calculations',
    current_inventory INT DEFAULT 0,  -- For inventory turnover metrics
    supplier_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- SCD tracking
    updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- SCD tracking
    INDEX idx_category (category),  -- Optimize for category-based queries
    INDEX idx_supplier (supplier_id)
) COMMENT 'Product dimension table following star schema design';

-- Fact table recording order transactions
-- Grain: One row per order
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,  -- Foreign key to customer dimension (not shown)
    order_date TIMESTAMP NOT NULL,  -- Time dimension for time-series analysis
    status VARCHAR(20) NOT NULL CHECK (status IN ('completed','returned','canceled','shipped')),
    shipping_cost DECIMAL(10,2) DEFAULT 0,
    payment_method VARCHAR(50),  -- Dimension attribute
    INDEX idx_order_date (order_date),  -- Critical for time-based queries
    INDEX idx_customer (customer_id)
) COMMENT 'Order fact table at order grain';

-- Fact table at order item grain (order line items)
-- Contains quantitative measures (quantity, price) for analysis
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),  -- Positive quantity constraint
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),  -- Non-negative price
    returned BOOLEAN DEFAULT FALSE,  // For return rate calculations
    return_reason VARCHAR(100),  // Categorical dimension for root cause analysis
    FOREIGN KEY (order_id) REFERENCES orders(order_id),  // Fact-to-fact relationship
    FOREIGN KEY (product_id) REFERENCES products(product_id),  // Fact-to-dimension
    INDEX idx_product (product_id),  // Optimize for product-based analysis
    INDEX idx_order (order_id)
) COMMENT 'Order items fact table at line item grain';

/*
ANALYTICS MART DESIGN:
- Pre-aggregated metrics at product/day grain
- Materialized pattern for dashboard performance
- Contains derived metrics for immediate analysis
*/
CREATE TABLE product_performance_daily (
    metric_date DATE NOT NULL,  // Time dimension at daily grain
    product_id INT NOT NULL,   // Product dimension key
    gross_revenue DECIMAL(12,2) NOT NULL,  // Measure
    avg_7d_revenue DECIMAL(12,2),  // Derived measure (window function)
    return_rate DECIMAL(5,4),  // Derived measure (calculated ratio)
    inventory_days DECIMAL(5,1),  // Derived measure (business KPI)
    PRIMARY KEY (metric_date, product_id),  // Composite key
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) COMMENT 'Product performance mart for dashboard consumption';

-- =============================================
-- SECTION 2: SAMPLE DATA INSERTION
-- =============================================

/*
DATA GENERATION NOTES:
- Representative sample covering multiple product categories
- Realistic time-series data for trend analysis
- Includes edge cases (returns, different order sizes)
- Timestamps relative to current date for freshness
*/
INSERT INTO products (product_id, product_name, category, price, cost, current_inventory) VALUES
(101, 'Premium Wireless Headphones', 'Electronics', 199.99, 120.00, 150),
(102, 'Organic Cotton T-Shirt', 'Apparel', 24.99, 8.50, 300),
(103, 'Stainless Steel Water Bottle', 'Home', 29.95, 12.00, 200),
(104, 'Yoga Mat', 'Fitness', 49.99, 18.75, 75),
(105, 'Bluetooth Speaker', 'Electronics', 89.99, 45.00, 120);

-- Generate orders across an 8-day period with realistic patterns
INSERT INTO orders VALUES
(1001, 1, CURRENT_DATE - INTERVAL 5 DAY + INTERVAL '9:15' HOUR_MINUTE, 'completed', 5.99, 'credit_card'),
(1002, 2, CURRENT_DATE - INTERVAL 2 DAY + INTERVAL '14:30' HOUR_MINUTE, 'completed', 0.00, 'paypal'),
/* ... additional orders ... */;

-- Generate order items with realistic quantities and some returns
INSERT INTO order_items VALUES
(1, 1001, 101, 1, 199.99, FALSE, NULL),  // Standard line item
(4, 1002, 104, 1, 49.99, TRUE, 'wrong size'),  // Returned item
/* ... additional order items ... */;

-- =============================================
-- SECTION 3: ETL PIPELINE IMPLEMENTATION
-- =============================================

/*
ETL PROCESS DESIGN:
1. EXTRACT: Source data from fact tables with date filter
2. TRANSFORM: 
   - Aggregations to product/day grain
   - Window functions for moving averages
   - Derived metric calculations
3. LOAD: Upsert pattern into analytics mart

PERFORMANCE CONSIDERATIONS:
- Limited date range for incremental processing
- CTEs for logical processing steps
- Window functions avoid expensive self-joins
*/
INSERT INTO product_performance_daily
WITH daily_sales AS (
  /* 
  EXTRACT & TRANSFORM:
  - Filter to completed orders in date range
  - Aggregate to product/day grain
  - Calculate base metrics (revenue, returns)
  */
  SELECT
    oi.product_id,
    DATE(o.order_date) AS metric_date,  // Standardize to date grain
    SUM(oi.quantity * oi.price) AS gross_revenue,  // Revenue calculation
    SUM(oi.quantity) AS total_units,
    SUM(CASE WHEN oi.returned THEN oi.quantity ELSE 0 END) AS returned_units  // Conditional aggregation
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  WHERE 
    o.order_date >= CURRENT_DATE - INTERVAL 8 DAY  // Incremental date range
    AND o.status = 'completed'  // Business filter
  GROUP BY 1, 2  // Product_id and date
),

with_rolling_metrics AS (
  /*
  ADVANCED TRANSFORMATIONS:
  - 7-day moving average using window frame
  - Return rate calculation with NULL protection
  - Window functions operate within product partitions
  */
  SELECT
    product_id,
    metric_date,
    gross_revenue,
    AVG(gross_revenue) OVER (
      PARTITION BY product_id  // Calculate per product
      ORDER BY metric_date    // Chronological ordering
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW  // 7-day window
    ) AS avg_7d_revenue,
    returned_units / NULLIF(total_units, 0) AS return_rate  // NULLIF prevents divide-by-zero
  FROM daily_sales
)

/*
FINAL LOAD:
- Join with dimension for inventory context
- Calculate business-ready metrics
- Upsert pattern prevents duplicates
*/
SELECT
  w.metric_date,
  w.product_id,
  w.gross_revenue,
  w.avg_7d_revenue,
  w.return_rate,
  // Inventory days = current stock / avg daily sales rate
  p.current_inventory / NULLIF(w.avg_7d_revenue/p.price, 0) AS inventory_days  
FROM with_rolling_metrics w
JOIN products p ON w.product_id = p.product_id
WHERE w.metric_date >= CURRENT_DATE - INTERVAL 7 DAY  // Final date filter
ON DUPLICATE KEY UPDATE  // Idempotent operation
  gross_revenue = VALUES(gross_revenue),
  avg_7d_revenue = VALUES(avg_7d_revenue),
  return_rate = VALUES(return_rate),
  inventory_days = VALUES(inventory_days);

-- =============================================
-- SECTION 4: ANALYTICAL VIEWS CREATION
-- =============================================

/*
DASHBOARD VIEW DESIGN:
- Combines metrics with dimension attributes
- Adds derived calculations for immediate use
- Includes business-friendly labeling
- Optimized for BI tool consumption
*/
CREATE OR REPLACE VIEW vw_product_dashboard AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  pd.metric_date,
  pd.gross_revenue,
  pd.avg_7d_revenue,
  ROUND(pd.return_rate * 100, 2) AS return_pct,  // Percentage formatting
  pd.inventory_days,
  /*
  BUSINESS RULES:
  - Classification based on metric thresholds
  - Dynamic labeling for visualization
  */
  CASE
    WHEN pd.return_rate > 0.1 THEN 'High Returns'
    WHEN pd.inventory_days > 60 THEN 'Overstocked'
    WHEN pd.inventory_days < 7 THEN 'Low Stock'
    ELSE 'Normal'
  END AS status_flag,
  /*
  TIME INTELLIGENCE:
  - Week-over-week growth calculation
  - Year-over-year growth comparison
  - LAG with window framing
  */
  pd.gross_revenue / NULLIF(
    LAG(pd.gross_revenue, 7) OVER (
      PARTITION BY pd.product_id 
      ORDER BY pd.metric_date
    ), 0
  ) - 1 AS wow_growth,
  pd.gross_revenue / NULLIF(
    LAG(pd.gross_revenue, 365) OVER (
      PARTITION BY pd.product_id 
      ORDER BY pd.metric_date
    ), 0
  ) - 1 AS yoy_growth
FROM product_performance_daily pd
JOIN products p ON pd.product_id = p.product_id;

/*
================================================================================
EXECUTION INSTRUCTIONS:
1. Run entire script to initialize database
2. Schedule SECTION 3 (ETL) for daily execution
3. Connect BI tools to vw_product_dashboard
4. For testing: SELECT * FROM vw_product_dashboard ORDER BY metric_date DESC LIMIT 100;

THEORETICAL BENEFITS DEMONSTRATED:
- Star schema enables performant analytics
- Incremental processing reduces compute costs
- Derived metrics provide business value
- Window functions enable complex temporal analysis
================================================================================
*/
