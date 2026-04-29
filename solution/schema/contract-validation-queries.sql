-- Contract Validation Queries
-- Purpose: Audit source schema data quality and validate assumptions
-- Source: operations schema (orders, order_items, products, customers)
-- Date: 2026-04-29

-- Query 1: Count rows per table
SELECT 'orders' as table_name, COUNT(*) as row_count FROM operations.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM operations.order_items
UNION ALL
SELECT 'products', COUNT(*) FROM operations.products
UNION ALL
SELECT 'customers', COUNT(*) FROM operations.customers;

-- Query 2: Check for NULL values in critical columns (orders)
SELECT
    'orders.status' as column_check,
    COUNT(*) FILTER (WHERE status IS NULL) as null_count,
    COUNT(*) as total_rows
FROM operations.orders
UNION ALL
SELECT 'orders.customer_id',
    COUNT(*) FILTER (WHERE customer_id IS NULL),
    COUNT(*)
FROM operations.orders
UNION ALL
SELECT 'orders.delivery_date',
    COUNT(*) FILTER (WHERE delivery_date IS NULL),
    COUNT(*)
FROM operations.orders;

-- Query 3: Check for NULL values in critical columns (order_items)
SELECT
    'order_items.order_id' as column_check,
    COUNT(*) FILTER (WHERE order_id IS NULL) as null_count,
    COUNT(*) as total_rows
FROM operations.order_items
UNION ALL
SELECT 'order_items.product_id',
    COUNT(*) FILTER (WHERE product_id IS NULL),
    COUNT(*)
FROM operations.order_items
UNION ALL
SELECT 'order_items.quanity',
    COUNT(*) FILTER (WHERE quanity IS NULL),
    COUNT(*)
FROM operations.order_items;

-- Query 4: Check for referential integrity (orders.customer_id → customers)
SELECT
    COUNT(*) as orders_with_missing_customer
FROM operations.orders o
WHERE o.customer_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM operations.customers c
      WHERE c.customer_id = o.customer_id
  );

-- Query 5: Check for referential integrity (order_items.order_id → orders)
SELECT
    COUNT(*) as order_items_with_missing_order
FROM operations.order_items oi
WHERE oi.order_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM operations.orders o
      WHERE o.order_id = oi.order_id
  );

-- Query 6: Check for referential integrity (order_items.product_id → products)
SELECT
    COUNT(*) as order_items_with_missing_product
FROM operations.order_items oi
WHERE oi.product_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM operations.products p
      WHERE p.product_id = oi.product_id
  );

-- Query 7: List distinct order.status values
SELECT DISTINCT status, COUNT(*) as count
FROM operations.orders
GROUP BY status
ORDER BY status;

-- Query 8: Sample 5 rows from each table (data quality spot check)
-- Sample: customers
SELECT * FROM operations.customers LIMIT 5;

-- Sample: products
SELECT * FROM operations.products LIMIT 5;

-- Sample: orders
SELECT * FROM operations.orders LIMIT 5;

-- Sample: order_items
SELECT * FROM operations.order_items LIMIT 5;
