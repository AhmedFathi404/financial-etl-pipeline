
-- raw transaction table
CREATE TABLE IF NOT EXISTS raw_transactions (
	id SERIAL PRIMARY KEY,
    transaction_discription TEXT,
    catigory VARCHAR(100),
    country VARCHAR(100),
    currency VARCHAR(3),
    amount DECIMAL(15,2),
    transaction_date TIMESTAMP,

);
    

-- daily aggregate table
CREATE TABLE IF NOT EXISTS daily_aggregates (
	id SERIAL PRIMARY KEY,
    transaction_date DATE,
    total_income DECIMAL(15,2),
    total_expense decimal(15,2),
    transaction_count INTEGER,
    created__at TIMESTAMP DEFAULT NOW()
    );


-- running balance table
CREATE TABLE IF NOT EXISTS running_balance (
    id SERIAL PRIMARY KEY,
    balance_date DATE,
    end_of_day_balance DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT NOW()
);



-- Data qulity table
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    check_date DATE,
    total_records INTEGER,
    null_count INTEGER,
    unknown_currency_count INTEGER,
    negative_amount_count INTEGER,
    income_expense_ratio DECIMAL(10,4),
    created_at TIMESTAMP DEFAULT NOW()
);


-- ETL logs table
CREATE TABLE IF NOT EXISTS etl_logs (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(100),
    status VARCHAR(20),
    records_processed INTEGER,
    error_message TEXT,
    execution_time_seconds DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW()
);
    