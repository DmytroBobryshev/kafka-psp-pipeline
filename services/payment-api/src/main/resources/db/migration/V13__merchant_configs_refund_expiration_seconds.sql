-- V13: merchant_configs.refund_expiration_seconds (M24) - the refund-path mirror of V11's
-- payment_expiration_seconds. How long (seconds since refunds.created_at) a refund may sit with
-- no terminal refund_status_history row (COMPLETED/FAILED/EXPIRED) before
-- adapters.in.scheduler.RefundExpirationScheduler moves it to EXPIRED (history-only - never
-- touches refunds.* itself, see domain.model.Refund's javadoc). DEFAULT 900 matches both
-- 06-merchant-config-changed.avsc's own Avro default AND
-- domain.model.MerchantConfig#DEFAULT_REFUND_EXPIRATION_SECONDS, so existing rows (written before
-- this column existed) get the same value a merchant with no config row at all falls back to via
-- the expiration query's COALESCE - no backfill needed, no behaviour change for any merchant that
-- has not yet re-PUT its config.

ALTER TABLE merchant_configs
    ADD COLUMN refund_expiration_seconds INT NOT NULL DEFAULT 900;
