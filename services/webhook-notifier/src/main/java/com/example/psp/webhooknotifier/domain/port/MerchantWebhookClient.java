package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DeliveryResult;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;

public interface MerchantWebhookClient {

    DeliveryResult deliver(WebhookDeliveryCommand command);
}
