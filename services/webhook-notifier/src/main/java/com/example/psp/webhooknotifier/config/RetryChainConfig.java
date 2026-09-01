package com.example.psp.webhooknotifier.config;

import com.example.psp.webhooknotifier.domain.model.RetryChain;
import java.time.Duration;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RetryChainConfig {

    @Bean
    public RetryChain retryChain(WebhookNotifierProperties properties) {
        WebhookNotifierProperties.Kafka kafka = properties.kafka();
        WebhookNotifierProperties.Retry retry = properties.retry();
        return new RetryChain(
                kafka.deliveryRequestedTopic(),
                List.of(
                        new RetryChain.Tier(kafka.retry5sTopic(), Duration.ofMillis(retry.delay5sMs())),
                        new RetryChain.Tier(kafka.retry1mTopic(), Duration.ofMillis(retry.delay1mMs())),
                        new RetryChain.Tier(kafka.retry15mTopic(), Duration.ofMillis(retry.delay15mMs()))),
                kafka.dlqTopic());
    }
}
