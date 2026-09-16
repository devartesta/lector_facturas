-- Speeds up PYG joins from monthly sales rows to their Shopify order.
CREATE INDEX CONCURRENTLY IF NOT EXISTS shopify_json_orders_name_idx
    ON shopify.json_orders ((raw_json ->> 'name'));
