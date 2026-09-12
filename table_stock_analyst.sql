-- Create a denormalizeed table for real-time stock analytics
CREATE TABLE IF NOT EXISTS realtime_stock_summary (
	id SERIAL PRIMARY KEY,
	symbol VARCHAR(50) NOT NULL,
	open_price NUMERIC(10,4) NOT NULL,
	high_price NUMERIC(10, 4) NOT NULL,
	low_price NUMERIC(10,4) NOT NULL,
	close_price NUMERIC(10, 4) NOT NULL,
	total_volume NUMERIC(15, 4) NOT NULL,
	processed_at TIMESTAMP NOT NULL
);

-- Create an index on the 'symbol' and 'processed_at' columns 
-- to make dashboard queries (e.g., filtering by stock and time) much faster.
CREATE INDEX idx_stock_symbol_time ON realtime_stock_summary (symbol, processed_at);