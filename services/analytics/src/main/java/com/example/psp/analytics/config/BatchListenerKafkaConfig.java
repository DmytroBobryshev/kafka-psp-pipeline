package com.example.psp.analytics.config;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer wiring for the M13 batch listener - a plain {@code @KafkaListener}, unrelated to the
 * Kafka Streams application in {@code config.KafkaStreamsConfig} beyond reading the same source
 * topic. Two settings here are the entire module: {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG}
 * (the batch-size lever) and {@code factory.setBatchListener(true)} (what turns the listener
 * method's parameter from one record into a {@code List}).
 */
@Configuration
public class BatchListenerKafkaConfig {

    @Bean
    public ConsumerFactory<String, PaymentStatusChanged> paymentStatusAuditConsumerFactory(
            KafkaProperties kafkaProperties, AnalyticsProperties properties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        // --- group.id: independent of streams.application-id -------------------------------
        // A second, unrelated consumer group on payments.payment-status-changed.v1 - see
        // AnalyticsProperties.BatchListener's javadoc for why that independence matters (this
        // listener's lag and this application's Streams lag are two different numbers, on two
        // different consumer groups, answering two different questions).
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.batchListener().groupId());

        // --- max.poll.records: THE batch-size lever -----------------------------------------
        // How many records land in the List<PaymentStatusChanged> the listener method receives
        // per invocation, and therefore how many documents one bulk Mongo write covers. See
        // application.yml's analytics.batch-listener.max-poll-records for the value and why it
        // is deliberately much larger than psp-connector's M4 max.poll.records=10.
        props.put(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.batchListener().maxPollRecords());

        // --- repo-wide consumer conventions (docs/diagrams/topic-map.md) ---------------------
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // --- Deserializers: ErrorHandlingDeserializer wraps the real ones (ADR-0006 category C,
        // same pattern as ledger's and psp-connector's Avro consumer factories) -----------------
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.schemaRegistry().url());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged>
            paymentStatusAuditBatchKafkaListenerContainerFactory(
                    @Qualifier("paymentStatusAuditConsumerFactory")
                            ConsumerFactory<String, PaymentStatusChanged> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // --- THE setting that makes this a batch listener -------------------------------------
        // Without this, the same @KafkaListener(topics = ...) would fail to start: Spring Kafka
        // validates that a List<T>-typed listener parameter is only legal on a container whose
        // factory has batch listening enabled.
        factory.setBatchListener(true);

        // --- AckMode.BATCH: one commit per successfully-processed batch, not per record --------
        // Consistent with "one bulk write per batch" - committing per record would put N broker
        // round trips back into the offset-commit path even after removing them from the Mongo
        // write path.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        // --- Error handling: the batch-specific difference --------------------------------------
        // DefaultErrorHandler recognises BatchListenerFailedException specifically (thrown by
        // adapters.in.kafka.PaymentStatusChangedBatchListener, translated from
        // domain.port.PartialBatchWriteException): it commits offsets for every record before the
        // exception's index and seeks the consumer back to redeliver only from that index onward.
        // A plain, unrecognised exception from a batch listener is treated as "the whole batch
        // failed" instead - FixedBackOff(1000ms, 3) bounds how many times that whole-batch retry
        // happens before the error handler gives up and logs, the same shape as every other
        // error handler in this codebase (ledger's, psp-connector's), not a DLQ - ADR-0006's real
        // policy for this listener is out of scope for M13, same documented gap as psp-connector's
        // M4 listener.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1_000L, 3L)));

        return factory;
    }
}
