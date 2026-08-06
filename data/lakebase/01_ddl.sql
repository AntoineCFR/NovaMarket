-- NovaMarket : schema de la base applicative (Lakebase Postgres)
-- A executer dans le SQL Editor de ton instance Lakebase.

DROP TABLE IF EXISTS app_customers;
DROP TABLE IF EXISTS app_sellers;

CREATE TABLE app_customers (
  customer_id   text PRIMARY KEY,
  first_name    text,
  last_name     text,
  email         text,
  country       text,
  city          text,
  zip_code      text,
  segment       text,
  is_opt_in     boolean,
  created_at    timestamp,
  updated_at    timestamp,
  is_deleted    boolean DEFAULT false
);

CREATE TABLE app_sellers (
  seller_id         text PRIMARY KEY,
  seller_name       text,
  seller_country    text,
  seller_city       text,
  main_top_category text,
  plan_code         text,
  is_active         boolean,
  onboarded_at      date,
  updated_at        timestamp
);

CREATE INDEX idx_customers_updated_at ON app_customers (updated_at);
CREATE INDEX idx_sellers_updated_at   ON app_sellers   (updated_at);
