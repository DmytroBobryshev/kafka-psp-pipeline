-- V11: merchant_configs.payment_expiration_seconds (M22). How long (seconds since
-- payments.created_at) a payment may sit in CREATED/PENDING before
-- adapters.in.scheduler.PaymentExpirationScheduler moves it to EXPIRED. DEFAULT 900 matches both
-- 06-merchant-config-changed.avsc's own Avro default AND
-- domain.model.MerchantConfig#DEFAULT_PAYMENT_EXPIRATION_SECONDS, so existing rows (written before
-- this column existed) get the same value a merchant with no config row at all falls back to via
-- the expiration query's COALESCE - no backfill needed, no behaviour change for any merchant that
-- has not yet re-PUT its config.

ALTER TABLE merchant_configs
    ADD COLUMN payment_expiration_seconds INT NOT NULL DEFAULT 900;
