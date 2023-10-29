-- Create Bars table
DROP TABLE IF EXISTS Bars_Dim;
CREATE TABLE IF NOT EXISTS Bars_Dim (
    bar_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city VARCHAR(100)
);

-- Create Glass table
DROP TABLE IF EXISTS Glass_Dim;
CREATE TABLE IF NOT EXISTS Glass_Dim (
    glass_id INTEGER PRIMARY KEY AUTOINCREMENT,
    glass_name VARCHAR(100)
);

-- Create Glass Stock table
DROP TABLE IF EXISTS Glass_Stock;
CREATE TABLE IF NOT EXISTS Glass_Stock (
    glass_id INTEGER,
    bar_id INTEGER,
    stock INTEGER,
    PRIMARY KEY (glass_id, bar_id),
    FOREIGN KEY (glass_id) REFERENCES Glass_Dim(glass_id),
    FOREIGN KEY (bar_id) REFERENCES Bars_Dim(bar_id)
);

-- Create Drinks table
DROP TABLE IF EXISTS Drinks_Dim;
CREATE TABLE IF NOT EXISTS Drinks_Dim (
    drink_id INTEGER PRIMARY KEY,
    drink_name VARCHAR(100),
    glass_id INTEGER,
    FOREIGN KEY (glass_id) REFERENCES Glass_Dim(glass_id)
);

-- Create Transactions table
DROP TABLE IF EXISTS Transactions;
CREATE TABLE IF NOT EXISTS Transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    drink_id INTEGER,
    amount FLOAT,
    bar_id INTEGER,
    timestamp TIMESTAMP,
    FOREIGN KEY (drink_id) REFERENCES Drinks_Dim(drink_id),
    FOREIGN KEY (bar_id) REFERENCES Bars_Dim(bar_id)
);