
ALTER TABLE merchant_configs
    ADD COLUMN payment_expiration_seconds INT NOT NULL DEFAULT 900;
