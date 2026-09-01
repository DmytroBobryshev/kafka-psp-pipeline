package com.example.psp.webhooknotifier.adapters.out.http;

import com.example.psp.webhooknotifier.config.WebhookNotifierProperties;
import com.example.psp.webhooknotifier.domain.model.DeliveryResult;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.model.WebhookUrlResolver;
import com.example.psp.webhooknotifier.domain.port.MerchantWebhookClient;
import com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

@Component
public class RestClientMerchantWebhookClient implements MerchantWebhookClient {

    private static final Logger log = LoggerFactory.getLogger(RestClientMerchantWebhookClient.class);

    private final RestClient restClient;
    private final String webhookPath;
    private final MerchantWebhookDirectory merchantWebhookDirectory;

    public RestClientMerchantWebhookClient(
            RestClient merchantWebhookRestClient,
            WebhookNotifierProperties properties,
            MerchantWebhookDirectory merchantWebhookDirectory) {
        this.restClient = merchantWebhookRestClient;
        this.webhookPath = properties.merchantClient().webhookPath();
        this.merchantWebhookDirectory = merchantWebhookDirectory;
    }

    @Override
    public DeliveryResult deliver(WebhookDeliveryCommand command) {
        WebhookCallbackRequest body =
                new WebhookCallbackRequest(
                        command.paymentId().toString(),
                        command.merchantId(),
                        command.amount(),
                        command.currency(),
                        command.status(),
                        command.declineReason(),
                        command.eventType(),
                        command.refundId() == null ? null : command.refundId().toString());

        String targetUri =
                WebhookUrlResolver.resolve(
                        merchantWebhookDirectory.findWebhookUrl(command.merchantId()), webhookPath);
        log.debug("Delivering webhook merchantId={} paymentId={} targetUri={}", command.merchantId(), command.paymentId(), targetUri);

        try {
            var response =
                    restClient
                            .post()
                            .uri(targetUri, command.merchantId())
                            .body(body)
                            .retrieve()
                            .toBodilessEntity();
            return DeliveryResult.success(response.getStatusCode().value());
        } catch (HttpClientErrorException e) {
            log.warn("Merchant returned 4xx merchantId={} paymentId={} status={}", command.merchantId(), command.paymentId(), e.getStatusCode());
            return DeliveryResult.nonRetryable(e.getStatusCode().value(), e.getMessage());
        } catch (HttpServerErrorException e) {
            log.warn("Merchant returned 5xx merchantId={} paymentId={} status={}", command.merchantId(), command.paymentId(), e.getStatusCode());
            return DeliveryResult.retryable(e.getStatusCode().value(), e.getMessage());
        } catch (ResourceAccessException e) {
            log.warn("Merchant call failed (timeout/connection) merchantId={} paymentId={}", command.merchantId(), command.paymentId(), e);
            return DeliveryResult.retryable(null, e.getMessage());
        }
    }
}
