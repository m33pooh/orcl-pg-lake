-- Initialization script for PostgreSQL Data Lake

CREATE SCHEMA IF NOT EXISTS datalake;

-- 1. Cold Orders Archive (E-Commerce)
CREATE TABLE IF NOT EXISTS datalake.orders_archive (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(30),
    order_created_at TIMESTAMP WITH TIME ZONE,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Cold Transactions Archive (Wallet)
CREATE TABLE IF NOT EXISTS datalake.wallet_transactions_archive (
    txn_id BIGINT PRIMARY KEY,
    wallet_id BIGINT NOT NULL,
    txn_type VARCHAR(20) NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    status VARCHAR(20),
    txn_created_at TIMESTAMP WITH TIME ZONE,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Cold Support Tickets Archive (CRM)
CREATE TABLE IF NOT EXISTS datalake.crm_tickets_archive (
    ticket_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    subject TEXT NOT NULL,
    status VARCHAR(30),
    priority VARCHAR(20),
    ticket_created_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Cold Audit Logs Archive (Compliance / Security)
CREATE TABLE IF NOT EXISTS datalake.audit_logs_archive (
    log_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    action VARCHAR(100) NOT NULL,
    resource TEXT NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    request_payload JSONB,
    log_created_at TIMESTAMP WITH TIME ZONE,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lake_orders_date ON datalake.orders_archive(order_created_at);
CREATE INDEX IF NOT EXISTS idx_lake_txn_date ON datalake.wallet_transactions_archive(txn_created_at);
CREATE INDEX IF NOT EXISTS idx_lake_audit_date ON datalake.audit_logs_archive(log_created_at);
