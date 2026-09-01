
ALTER TABLE merchant_configs
    ADD COLUMN refund_expiration_seconds INT NOT NULL DEFAULT 900;
