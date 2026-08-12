package com.example.psp.common.health;

import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Wires {@code common-health}'s indicators into any service that depends on this jar.
 *
 * <p><b>Off by default, on purpose.</b> Both properties default to {@code false}, and only the
 * Kubernetes ConfigMap (infra/k8s/charts/psp-platform/charts/&lt;service&gt;/values.yaml) turns
 * them on. A {@code HealthIndicator} bean contributes to the top-level {@code /actuator/health}
 * aggregate as well as to any group, so switching it on unconditionally would change what {@code
 * docker-compose}'s health checks and Eureka's instance status report - a Kubernetes task has no
 * business doing that to the compose profile. Under the {@code k8s} profile the change is the
 * entire point.
 *
 * <p><b>Two properties rather than one, and no {@code @ConditionalOnBean}.</b> The obvious
 * spelling - one flag plus {@code @ConditionalOnBean(StreamsBuilderFactoryBean.class)} so the
 * Streams indicator appears only where a topology exists - is a trap. {@code @ConditionalOnBean}
 * inside an auto-configuration is evaluated against whatever bean definitions happen to be
 * registered at that moment, which makes "does analytics get this bean" depend on
 * auto-configuration ordering rather than on anything the deployment said. And when it silently
 * resolves to "no", the failure is not silent at all: Spring Boot validates health group
 * membership at startup and refuses to boot with <em>Included health contributor 'kafkaStreams' in
 * group 'readiness' does not exist</em>. Making each indicator an explicit deployment decision
 * removes the ordering question entirely - the chart names the contributor in
 * {@code readinessInclude} and switches on the property that creates it, in the same values.yaml,
 * where the two cannot drift apart.
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
public class KafkaHealthAutoConfiguration {

    /**
     * @param registry always present in a service with spring-kafka on the classpath - Spring
     *     Boot's {@code @EnableKafka} registers it - so an injection failure here means the
     *     property was switched on for a service that has no Kafka client at all, which is worth
     *     failing over.
     */
    @Bean("kafkaListeners")
    @ConditionalOnProperty(name = "psp.health.kafka.enabled", havingValue = "true")
    public HealthIndicator kafkaListenersHealthIndicator(KafkaListenerEndpointRegistry registry) {
        return new KafkaListenerContainersHealthIndicator(registry);
    }

    @Bean("kafkaStreams")
    @ConditionalOnProperty(name = "psp.health.kafka.streams.enabled", havingValue = "true")
    public HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
        return new KafkaStreamsHealthIndicator(factoryBean);
    }
}
