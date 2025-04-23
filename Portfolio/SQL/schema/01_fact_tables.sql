-- Orders fact table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('completed','returned','canceled','shipped')),
    shipping_cost DECIMAL(10,2) DEFAULT 0,
    payment_method VARCHAR(50),
    INDEX idx_order_date (order_date),
    INDEX idx_customer (customer_id)
) COMMENT 'Fact table for all customer orders';

-- Order items fact table
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    returned BOOLEAN DEFAULT FALSE,
    return_reason VARCHAR(100),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    INDEX idx_product (product_id),
    INDEX idx_order (order_id)
) COMMENT 'Line items for each order';
