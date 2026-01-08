CREATE TABLE app_user_visits_fact (
    id String,
    phone_number String,
    seen Int32,
    state Int32,
    points Float64,
    receipt Float64,
    countryCode String,
    remaining Float64,
    customer_id String,
    branch_id String,
    store_id String,
    cashier_id String,
    created_at Int64,
    updated_at Int64,
    expired Int32,
    expires_at Int64,
    order_id String,
    is_deleted Int8,
    is_fraud Int8,
    sync_mechanism String,
    is_bulk_points String
) 
ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (id);
-- OR use ORDER BY (id);