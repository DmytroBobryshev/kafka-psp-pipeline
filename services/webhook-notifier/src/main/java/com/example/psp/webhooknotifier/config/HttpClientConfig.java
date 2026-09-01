package com.example.psp.webhooknotifier.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

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
