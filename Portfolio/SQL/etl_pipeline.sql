-- 1. CREATE TABLES WITH SAMPLE DATA

-- Products dimension table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    current_inventory INT,
    supplier_id INT
);

-- Customers dimension table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    join_date DATE,
    loyalty_tier VARCHAR(50)
);

-- Orders fact table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(50),
    shipping_cost DECIMAL(10, 2),
    payment_method VARCHAR(50)
);

-- Order items fact table
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    returned BOOLEAN DEFAULT FALSE,
    return_reason VARCHAR(255)
);

-- 2. INSERT SAMPLE DATA

-- Insert products
INSERT INTO products VALUES
(101, 'Premium Wireless Headphones', 'Electronics', 199.99, 150, 1),
(102, 'Organic Cotton T-Shirt', 'Apparel', 24.99, 300, 2),
(103, 'Stainless Steel Water Bottle', 'Home', 29.95, 200, 3),
(104, 'Yoga Mat', 'Fitness', 49.99, 75, 4),
(105, 'Bluetooth Speaker', 'Electronics', 89.99, 120, 1);

-- Insert customers
INSERT INTO customers VALUES
(1, 'Sarah', 'Johnson', 'sarah.j@email.com', '2022-01-15', 'Gold'),
(2, 'Michael', 'Chen', 'michael.c@email.com', '2022-03-22', 'Silver'),
(3, 'Emily', 'Wilson', 'emily.w@email.com', '2021-11-05', 'Platinum'),
(4, 'David', 'Brown', 'david.b@email.com', '2023-02-10', 'Bronze');

-- Insert orders (last 30 days of data)
INSERT INTO orders VALUES
(1001, 1, CURRENT_DATE - INTERVAL '5 days' + INTERVAL '9 hours 15 minutes', 'completed', 5.99, 'credit_card'),
(1002, 2, CURRENT_DATE - INTERVAL '12 days' + INTERVAL '14 hours 30 minutes', 'completed', 0.00, 'paypal'),
(1003, 3, CURRENT_DATE - INTERVAL '3 days' + INTERVAL '11 hours 45 minutes', 'completed', 5.99, 'credit_card'),
(1004, 1, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '16 hours 20 minutes', 'completed', 0.00, 'apple_pay'),
(1005, 4, CURRENT_DATE - INTERVAL '8 days' + INTERVAL '10 hours 10 minutes', 'completed', 5.99, 'credit_card'),
(1006, 2, CURRENT_DATE - INTERVAL '15 days' + INTERVAL '13 hours 25 minutes', 'completed', 0.00, 'paypal');

-- Insert order items (with some returns)
INSERT INTO order_items VALUES
-- Order 1001
(1, 1001, 101, 1, 199.99, FALSE, NULL),
(2, 1001, 103, 2, 29.95, FALSE, NULL),

-- Order 1002
(3, 1002, 102, 3, 24.99, FALSE, NULL),
(4, 1002, 104, 1, 49.99, TRUE, 'wrong size'),

-- Order 1003
(5, 1003, 105, 1, 89.99, FALSE, NULL),
(6, 1003, 101, 1, 199.99, FALSE, NULL),

-- Order 1004
(7, 1004, 102, 2, 24.99, FALSE, NULL),
(8, 1004, 103, 1, 29.95, FALSE, NULL),

-- Order 1005
(9, 1005, 104, 1, 49.99, FALSE, NULL),
(10, 1005, 105, 1, 89.99, TRUE, 'defective'),

-- Order 1006
(11, 1006, 101, 1, 199.99, FALSE, NULL),
(12, 1006, 102, 1, 24.99, FALSE, NULL);

-- 3. CREATE INDEXES FOR PERFORMANCE
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);

-- 4. EXECUTE THE ETL PIPELINE
This script demonstrates a complete data transformation pipeline that:
1. Extracts raw order data
2. Transforms it into meaningful business metrics
3. Loads it into an analytical model ready for visualization

/*Theoretical Concepts:
- Dimensional Modeling: We're creating a star schema with product as the fact table
- Time Intelligence: Calculating moving averages and YoY comparisons
- Data Quality: Using NULLIF to handle potential divide-by-zero errors
- Performance: Window functions allow efficient calculations without self-joins
*/

-- STAGE 1: Extract and aggregate raw data at the product/day grain
WITH daily_product_metrics AS (
  /*
  This CTE performs the initial aggregation from transactional data to daily metrics.
  
  Key Transformations:
  - Date truncation to create consistent time buckets
  - Multiplicative calculation (price * quantity) for revenue
  - Conditional aggregation for returns tracking
  
  Data Warehouse Concept:
  - This creates the "fact table" at the lowest grain we'll need (product/day)
  */
  SELECT
    product_id,
    DATE_TRUNC('day', order_date) AS day, -- Standardizes timestamps to daily buckets
    COUNT(DISTINCT order_id) AS order_count, -- Count of unique orders containing this product
    SUM(quantity) AS total_quantity, -- Total units sold
    SUM(quantity * price) AS gross_revenue, -- Price * Quantity calculation
    SUM(CASE WHEN returned THEN quantity ELSE 0 END) AS returned_quantity -- Conditional sum
  FROM orders
  JOIN order_items USING (order_id) -- Relational join to get product details
  GROUP BY 1, 2 -- Group by product_id and day
),

-- STAGE 2: Add time-based calculations
rolling_metrics AS (
  /*
  This CTE adds time-oriented analytics using window functions.
  
  Analytical Concepts:
  - Window frames (7-day range) for moving averages
  - YoY growth calculation using date-based offset (LAG 365 days)
  - NULLIF pattern to avoid divide-by-zero errors
  
  Performance Note:
  - Window functions process the entire partition at once, more efficient than self-joins
  */
  SELECT
    product_id,
    day,
    gross_revenue,
    -- 7-day moving average using window frame
    AVG(gross_revenue) OVER (
      PARTITION BY product_id 
      ORDER BY day 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW -- Fixed window of 7 days (6 preceding + current)
    ) AS avg_7day_revenue,
    
    -- Return rate with safety check for zero denominator
    returned_quantity / NULLIF(total_quantity, 0) AS return_rate,
    
    -- Year-over-year growth calculation
    gross_revenue / NULLIF(
      LAG(gross_revenue, 365) OVER (PARTITION BY product_id ORDER BY day), -- Look back exactly 1 year
      0
    ) - 1 AS yoy_growth
  FROM daily_product_metrics
)

-- STAGE 3: Final presentation layer
/*
This is the final SELECT that would feed a dashboard or report.
It joins our metrics with dimension tables for business context.

Data Modeling Concept:
- This represents the "mart" layer where we:
  - Filter to only the most recent complete day
  - Add dimensional attributes (product name, category)
  - Select only columns needed for analysis
*/
SELECT
  r.product_id,
  r.day AS metric_date,
  p.product_name,
  p.category,
  r.gross_revenue,
  r.avg_7day_revenue,
  r.return_rate,
  r.yoy_growth,
  p.current_inventory,
  -- Derived metric: Inventory days based on recent sales rate
  p.current_inventory / NULLIF(r.avg_7day_revenue / p.price, 0) AS inventory_days
FROM rolling_metrics r
JOIN products p USING (product_id)
WHERE day = CURRENT_DATE - INTERVAL '1 day' -- Common practice to analyze complete days
ORDER BY r.gross_revenue DESC;