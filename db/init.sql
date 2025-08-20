CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(100)
);

INSERT INTO users (name, country) VALUES
('Atakan', 'Türkiye'),
('Alice', 'UK'),
('Bob', 'USA');
