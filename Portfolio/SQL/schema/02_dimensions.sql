-- Products dimension table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    cost DECIMAL(10,2) COMMENT 'Manufacturing cost',
    current_inventory INT DEFAULT 0,
    supplier_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_supplier (supplier_id)
) COMMENT 'Product master data';

-- Create product performance mart
CREATE TABLE product_performance_daily (
    metric_date DATE NOT NULL,
    product_id INT NOT NULL,
    gross_revenue DECIMAL(12,2) NOT NULL,
    avg_7d_revenue DECIMAL(12,2),
    return_rate DECIMAL(5,4),
    inventory_days DECIMAL(5,1),
    PRIMARY KEY (metric_date, product_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) COMMENT 'Daily product performance metrics';
