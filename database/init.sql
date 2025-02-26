-- Create and setup TimescaleDB for payment system
-- This will be executed when the database initializes

-- Connect to the payment_system database
\c payment_system;

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create transactions table
CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    status TEXT NOT NULL,
    device_type TEXT NOT NULL,
    time TIMESTAMP NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    country TEXT,
    city TEXT,
    merchant_id TEXT,
    merchant_category TEXT,
    description TEXT,
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_score FLOAT
);

-- Convert transactions to hypertable partitioned on time
SELECT create_hypertable('transactions', 'time');

-- Create indexes
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_payment_method ON transactions(payment_method);
CREATE INDEX idx_transactions_is_fraud ON transactions(is_fraud);
CREATE INDEX idx_transactions_merchant_category ON transactions(merchant_category);
CREATE INDEX idx_transactions_city ON transactions(city);

-- Create table for fraud events
CREATE TABLE fraud_events (
    event_id SERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL REFERENCES transactions(transaction_id),
    time TIMESTAMP NOT NULL,
    fraud_score FLOAT NOT NULL,
    details JSONB,
    is_confirmed BOOLEAN DEFAULT FALSE,
    resolution_status TEXT DEFAULT 'pending'
);

-- Convert fraud_events to hypertable
SELECT create_hypertable('fraud_events', 'time');

-- Create view for transaction summaries by time bucket
CREATE VIEW transaction_summary AS
SELECT 
    time_bucket('1 hour', time) AS bucket,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN is_fraud THEN 1 END) AS fraud_transactions,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    payment_method,
    city
FROM transactions
GROUP BY bucket, payment_method, city;

-- Create view for fraud metrics
CREATE VIEW fraud_metrics AS
SELECT 
    time_bucket('1 day', time) AS day,
    payment_method,
    COUNT(*) AS fraud_count,
    AVG(fraud_score) AS avg_fraud_score,
    SUM(amount) AS total_fraud_amount
FROM transactions
WHERE is_fraud = TRUE
GROUP BY day, payment_method
ORDER BY day DESC, fraud_count DESC;

-- Create retention policy to automatically drop old data
-- Keep transaction data for 90 days
SELECT add_retention_policy('transactions', INTERVAL '90 days');

-- Keep fraud events for 365 days
SELECT add_retention_policy('fraud_events', INTERVAL '365 days');

-- Create a user for Grafana access
CREATE USER grafana WITH PASSWORD 'grafana_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana;