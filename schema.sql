-- schema.sql
-- Drop tables if they exist
DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS banks;

-- -----------------------
-- Banks table
-- -----------------------
CREATE TABLE banks (
    bank_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- -----------------------
-- Reviews table
-- -----------------------
CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    bank_id INT NOT NULL REFERENCES banks(bank_id) ON DELETE CASCADE,
    review TEXT NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    review_date DATE,
    source TEXT,
    CONSTRAINT unique_review_row UNIQUE (bank_id, review, review_date)
);

-- -----------------------
-- Indexes (optional but recommended)
-- -----------------------
CREATE INDEX idx_reviews_bank ON reviews(bank_id);
CREATE INDEX idx_reviews_date ON reviews(review_date);
