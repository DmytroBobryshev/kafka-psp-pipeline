/**
 * Spring configuration, bean wiring, and profiles (ADR-0007): explicit producer/consumer factory
 * wiring ({@link com.example.psp.webhooknotifier.config.KafkaProducerConfig},
 * {@link com.example.psp.webhooknotifier.config.KafkaConsumerConfig}), the retry-chain topology
 * ({@link com.example.psp.webhooknotifier.config.RetryChainConfig}), the Mongo TTL index
 * ({@link com.example.psp.webhooknotifier.config.MongoIndexConfig}), the merchant HTTP client
 * ({@link com.example.psp.webhooknotifier.config.HttpClientConfig}), and the
 * {@code webhook-notifier.*} / {@code webhook-notifier.simulated-merchant.*} property groups.
 */
package com.example.psp.webhooknotifier.config;
