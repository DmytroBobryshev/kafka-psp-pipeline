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

/**
 * Real HTTP adapter for {@link MerchantWebhookClient} (ADR-0004's outbound-HTTP carve-out, same
 * as psp-connector's {@code SimulatedPaymentProviderAdapter} for the provider call).
 *
 * <h2>Target URL resolution - the M8 bug fix</h2>
 *
 * <p>{@link WebhookUrlResolver} decides the target on every call: if {@link MerchantWebhookDirectory}
 * has a projected {@code webhookUrl} for this merchant, {@code restClient}'s absolute-URI handling
 * (Spring's {@code DefaultUriBuilderFactory} ignores the configured base URL whenever the given
 * URI template already has a host) sends the request straight there; otherwise the original
 * relative template resolves against {@code webhook-notifier.merchant-client.base-url} exactly as
 * before - by default {@code adapters.in.web.SimulatedMerchantController} in this same process,
 * over real loopback HTTP. Resolved fresh on every attempt (not cached on the command at planning
 * time), so a merchant's most recently configured URL always wins.
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
            // Connect timeout, read timeout, connection refused/reset - no HTTP response was ever
            // received, so there is no status code to report. This is the branch
            // adapters.in.web.SimulatedMerchantController's TIMEOUT simulation exercises.
            log.warn("Merchant call failed (timeout/connection) merchantId={} paymentId={}", command.merchantId(), command.paymentId(), e);
            return DeliveryResult.retryable(null, e.getMessage());
        }
    }
}
