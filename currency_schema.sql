-- Створюємо довідник
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id INTEGER PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(50) NOT NULL
);

-- Створюємо таблицю з курсами
CREATE TABLE IF NOT EXISTS fact_exchange_rate (
    rate_date DATE NOT NULL,
    currency_id INT NOT NULL,
    rate DECIMAL(10, 4) NOT NULL,
    FOREIGN KEY (currency_id) REFERENCES dim_currency(currency_id),
    PRIMARY KEY (rate_date, currency_id) -- Складений ключ, щоб не було дублів за один день
);

CREATE INDEX IF NOT EXISTS idx_fact_exchange_rate_currency_id ON fact_exchange_rate(currency_id);
CREATE INDEX IF NOT EXISTS idx_fact_exchange_rate_rate_date ON fact_exchange_rate(rate_date);
CREATE INDEX IF NOT EXISTS idx_dim_currency_currency_code ON dim_currency(currency_code);