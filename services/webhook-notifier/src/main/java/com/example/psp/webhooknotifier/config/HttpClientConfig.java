package com.example.psp.webhooknotifier.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * The {@link RestClient} {@code adapters.out.http.RestClientMerchantWebhookClient} uses to call
 * the merchant endpoint over real HTTP. Connect/read timeouts come from
 * {@code webhook-notifier.merchant-client.*} - the read timeout in particular is what turns
 * {@code adapters.in.web.SimulatedMerchantController}'s deliberately slow TIMEOUT simulation into
 * a genuine client-side {@code DeliveryOutcome.RETRYABLE_FAILURE} (see that controller's javadoc
 * for the timing relationship that must hold between the two properties).
 */
@Configuration
public class HttpClientConfig {

    @Bean
    public RestClient merchantWebhookRestClient(WebhookNotifierProperties properties) {
        WebhookNotifierProperties.MerchantClient config = properties.merchantClient();

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout((int) config.connectTimeoutMs());
        requestFactory.setReadTimeout((int) config.readTimeoutMs());

        return RestClient.builder().baseUrl(config.baseUrl()).requestFactory(requestFactory).build();
    }
}
