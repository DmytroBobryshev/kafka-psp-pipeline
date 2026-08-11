package com.example.psp.webhooknotifier.adapters.out.http;

import com.example.psp.webhooknotifier.config.WebhookNotifierProperties;
import com.example.psp.webhooknotifier.domain.model.DeliveryResult;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.MerchantWebhookClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * Real HTTP adapter for {@link MerchantWebhookClient} (ADR-0004's outbound-HTTP carve-out, same
 * as psp-connector's {@code SimulatedPaymentProviderAdapter} for the provider call). Calls
 * {@code webhook-notifier.merchant-client.base-url + webhook-path} - by default
 * {@code adapters.in.web.SimulatedMerchantController} in this same process, over real loopback
 * HTTP.
 *
 * <h2>Classification (ADR-0006, applied to an outbound HTTP call)</h2>
 *
 * <table>
 *   <caption>HTTP outcome to DeliveryOutcome</caption>
 *   <tr><th>What happened</th><th>Exception</th><th>Outcome</th></tr>
 *   <tr><td>2xx</td><td>-</td><td>{@code SUCCESS}</td></tr>
 *   <tr><td>Merchant 5xx</td><td>{@code HttpServerErrorException}</td><td>{@code RETRYABLE_FAILURE}</td></tr>
 *   <tr><td>Connect/read timeout, connection refused</td><td>{@code ResourceAccessException}</td><td>{@code RETRYABLE_FAILURE}</td></tr>
 *   <tr><td>Merchant 4xx</td><td>{@code HttpClientErrorException}</td><td>{@code NON_RETRYABLE_FAILURE}</td></tr>
 *   <tr><td>Anything else (bad base-url, unexpected runtime failure)</td><td>propagates uncaught</td><td>n/a - ADR-0006 category D, handled at the Kafka container level, see {@code config.KafkaConsumerConfig}</td></tr>
 * </table>
 */
@Component
public class RestClientMerchantWebhookClient implements MerchantWebhookClient {

    private static final Logger log = LoggerFactory.getLogger(RestClientMerchantWebhookClient.class);

    private final RestClient restClient;
    private final String webhookPath;

    public RestClientMerchantWebhookClient(RestClient merchantWebhookRestClient, WebhookNotifierProperties properties) {
        this.restClient = merchantWebhookRestClient;
        this.webhookPath = properties.merchantClient().webhookPath();
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
                        command.declineReason());

        try {
            var response =
                    restClient
                            .post()
                            .uri(webhookPath, command.merchantId())
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
            // Connect timeout, read timeout, connection refused/reset - no HTTP response was ever
            // received, so there is no status code to report. This is the branch
            // adapters.in.web.SimulatedMerchantController's TIMEOUT simulation exercises.
            log.warn("Merchant call failed (timeout/connection) merchantId={} paymentId={}", command.merchantId(), command.paymentId(), e);
            return DeliveryResult.retryable(null, e.getMessage());
        }
    }
}
