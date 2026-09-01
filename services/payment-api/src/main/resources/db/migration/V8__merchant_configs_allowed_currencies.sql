ALTER TABLE merchant_configs
    ADD COLUMN allowed_currencies VARCHAR(64) NOT NULL DEFAULT '';
