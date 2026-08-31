-- Local read-model projection of merchants.merchant-config-changed.v1 (compacted topic is the
-- system of record - see domain.model.MerchantView). No FK to payments: merchant_id here is
-- whatever adapters.in.kafka.MerchantConfigChangedListener has consumed, independent of
-- payments.merchant_id.

CREATE TABLE merchant_configs (
    merchant_id                      VARCHAR(255)  PRIMARY KEY,
    display_name                     VARCHAR(255)  NOT NULL,
    status                           VARCHAR(20)   NOT NULL,
    payout_currency                  VARCHAR(3)    NOT NULL,
    webhook_url                      VARCHAR(2048),
    decline_rate_alert_threshold_bps INT           NOT NULL,
    updated_at                       TIMESTAMPTZ   NOT NULL
);

-- GET /api/merchants?status= filters on this column.
CREATE INDEX idx_merchant_configs_status ON merchant_configs (status);
