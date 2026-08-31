-- CSV of 1-3 ISO-4217 codes a merchant may be paid in (see domain.model.MerchantConfig).
-- DEFAULT '' matches the Avro field's own default ([] -> ""): an empty value means "legacy
-- record - fall back to payout_currency", so existing rows stay valid without a backfill.
ALTER TABLE merchant_configs
    ADD COLUMN allowed_currencies VARCHAR(64) NOT NULL DEFAULT '';
