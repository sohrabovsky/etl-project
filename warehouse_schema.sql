CREATE TABLE IF NOT EXISTS dim_customers(
customer_id VARCHAR PRIMARY KEY,
name TEXT,
email TEXT,
country TEXT,
signup_date DATE
);

CREATE TABLE IF NOT EXISTS dim_products(
product_id VARCHAR PRIMARY KEY,
name TEXT,
category TEXT,
price NUMERIC
);

CREATE TABLE IF NOT EXISTS fact_sales(
order_id VARCHAR,
customer_id VARCHAR REFERENCES dim_customers(customer_id),
product_id VARCHAR REFERENCES dim_products(product_id),
quantity INT,
total_amount NUMERIC,
order_date DATE
);

