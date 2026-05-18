

CREATE DATABASE IF NOT EXISTS orders_analytics;


CREATE TABLE IF NOT EXISTS orders_analytics.orders (
    order_id                UInt32,
    user_id                 UInt32,
    order_number            UInt16,
    order_dow               UInt8,         -- 0=Minggu, 1=Senin, ..., 6=Sabtu
    order_hour_of_day       UInt8,         -- 0-23
    days_since_prior_order  Nullable(Float32), -- NULL untuk order pertama
    eval_set                String,
    product_count           UInt16         -- jumlah produk dalam order
) ENGINE = MergeTree()
ORDER BY order_id;



CREATE TABLE IF NOT EXISTS orders_analytics.order_products (
    order_id            UInt32,
    product_id          UInt32,
    product_name        String,
    aisle_id            UInt16,
    aisle               String,
    department_id       UInt16,
    department          String,
    add_to_cart_order   UInt16,   -- urutan masuk keranjang
    reordered           UInt8     -- 1 = pernah dipesan sebelumnya
) ENGINE = MergeTree()
ORDER BY (order_id, product_id);



CREATE TABLE IF NOT EXISTS orders_analytics.top_products (
    product_id      UInt32,
    product_name    String,
    order_count     UInt32,
    reorder_count   UInt32,
    reorder_rate    Float32    -- persentase reorder (0-100)
) ENGINE = MergeTree()
ORDER BY order_count;



CREATE TABLE IF NOT EXISTS orders_analytics.department_stats (
    department_id            UInt16,
    department               String,
    total_products_ordered   UInt32,
    unique_products          UInt32,
    reorder_count            UInt32,
    order_count              UInt32,
    reorder_rate             Float32
) ENGINE = MergeTree()
ORDER BY total_products_ordered;


-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders_analytics.aisle_stats (
    aisle_id                UInt16,
    aisle                   String,
    department              String,
    total_products_ordered  UInt32,
    unique_products         UInt32
) ENGINE = MergeTree()
ORDER BY total_products_ordered;



CREATE TABLE IF NOT EXISTS orders_analytics.hourly_distribution (
    order_hour_of_day   UInt8,
    order_count         UInt32
) ENGINE = MergeTree()
ORDER BY order_hour_of_day;



CREATE TABLE IF NOT EXISTS orders_analytics.dow_distribution (
    order_dow       UInt8,     -- 0=Minggu ... 6=Sabtu
    order_count     UInt32
) ENGINE = MergeTree()
ORDER BY order_dow;
