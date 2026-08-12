package com.example.psp.pspconnector.config;

import com.example.psp.pspconnector.adapters.in.kafka.PaymentRequestedEvent;
import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * EXPERIMENT-ONLY wiring for the README's "duplicates vs loss" drill, active only under the
 * {@code auto-commit-drill} Spring profile - never on by default, never used in production.
 *
 * <p>The production path ({@link KafkaConsumerConfig}) hardcodes
 * {@code enable.auto.commit=false} and manual ack, which is the entire point of this module - see
 * that class's javadoc. Spring Kafka's manual {@code Acknowledgment} API only exists when the
 * container manages commits itself; once the Kafka client's OWN {@code enable.auto.commit=true}
 * is in play, Spring Kafka steps back entirely and the client's internal auto-commit thread takes
 * over, on its own timer ({@code auto.commit.interval.ms}, default 5000ms), regardless of
 * anything the listener does. Comparing the two commit strategies for real therefore needs a
 * second, real listener wired the other way - not a mock or a thought experiment - which is what
 * {@link com.example.psp.pspconnector.adapters.in.kafka.AutoCommitDriftListener} is.
 *
 * <p><b>Stale as of M9 Phase 1:</b> this factory still deserializes
 * {@code payments.payment-requested.v1} as JSON via {@code PaymentRequestedEvent} - see that
 * class's javadoc. The topic itself now carries Avro-encoded bytes (Confluent wire format), so
 * running this drill against the live cluster would poison-pill on every record. Left unchanged
 * deliberately: it is an M4-era experiment behind a profile that is off by default and out of M9
 * Phase 1's scope (one topic's production consumer, not every experimental listener that ever
 * subscribed to it) - a future phase would need to point this at the generated Avro type too.
 */
@Configuration
@Profile("auto-commit-drill")
public class KafkaAutoCommitDriftConfig {

    @Bean
    public ConsumerFactory<String, PaymentRequestedEvent> autoCommitDriftConsumerFactory(
            KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        // The one property this whole drill exists to flip - see the class javadoc.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        // Same deserializer safety net as the production factory - not the point of this drill,
        // but there's no reason to let a poison pill crash the comparison.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentRequestedEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.psp.*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentRequestedEvent>
            autoCommitDriftKafkaListenerContainerFactory(
                    @Qualifier("autoCommitDriftConsumerFactory")
                            ConsumerFactory<String, PaymentRequestedEvent> autoCommitDriftConsumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentRequestedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(autoCommitDriftConsumerFactory);
        // AckMode is deliberately left at its default: with enable.auto.commit=true on the
        // consumer itself, Spring Kafka's own commit management (whatever AckMode said) never
        // engages - see the class javadoc. Nothing to configure here on purpose.
        // M15: enabled for consistency with the production listener, even though this drill
        // profile is off by default (see class javadoc).
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
