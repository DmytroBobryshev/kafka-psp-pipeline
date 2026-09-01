
CREATE TABLE merchant_configs (
    merchant_id                      VARCHAR(255)  PRIMARY KEY,
    display_name                     VARCHAR(255)  NOT NULL,
    status                           VARCHAR(20)   NOT NULL,
    payout_currency                  VARCHAR(3)    NOT NULL,
    webhook_url                      VARCHAR(2048),
    decline_rate_alert_threshold_bps INT           NOT NULL,
    updated_at                       TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_merchant_configs_status ON merchant_configs (status);
