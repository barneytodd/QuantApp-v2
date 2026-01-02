CREATE TABLE dbo.prices (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,

    [open] FLOAT NOT NULL,
    [high] FLOAT NOT NULL,
    [low] FLOAT NOT NULL,
    [close] FLOAT NOT NULL,
    [volume] BIGINT NOT NULL,

    CONSTRAINT uix_symbol_date UNIQUE (symbol, date)
);

CREATE INDEX idx_prices_symbol_date
    ON prices (symbol, date);
