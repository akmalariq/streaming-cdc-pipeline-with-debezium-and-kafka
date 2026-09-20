CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    city TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers (customer_id),
    status TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders (order_id),
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO customers (customer_id, name, email, city) VALUES
    (1, 'Ada Lovelace', 'ada@example.com', 'Jakarta'),
    (2, 'Grace Hopper', 'grace@example.com', 'Bandung'),
    (3, 'Alan Turing', 'alan@example.com', 'Surabaya');

INSERT INTO orders (order_id, customer_id, status, amount) VALUES
    (1001, 1, 'paid', 250000.00),
    (1002, 2, 'pending', 175000.00),
    (1003, 1, 'shipped', 99000.00);

INSERT INTO order_items (order_item_id, order_id, sku, quantity, unit_price) VALUES
    (9001, 1001, 'SKU-ESPRESSO', 1, 250000.00),
    (9002, 1002, 'SKU-LATTE', 2, 87500.00),
    (9003, 1003, 'SKU-COLD-BREW', 1, 99000.00);
