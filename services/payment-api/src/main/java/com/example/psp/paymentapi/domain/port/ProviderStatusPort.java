package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import java.util.UUID;

/**
 * Outbound port for the M12 synchronous provider-status check. Implemented by
 * {@code adapters.out.kafka.ProviderStatusRequestGateway}, which sends
 * {@code psp.provider-status-query.v1} and blocks for
 * {@code psp.provider-status-reply.v1} via {@code ReplyingKafkaTemplate} - the domain never
 * imports Kafka or the generated Avro types directly (ADR-0007), only this interface and the
 * plain {@link ProviderStatusResult} it returns.
 *
 * <p>This is the ONE port in this service backed by a real-time network round trip rather than a
 * database read or an outbox write - see {@code config.ReplyingKafkaConfig}'s javadoc and the
 * README's M12 section for why that is a deliberate, narrow exception to ADR-0004's "no
 * service-to-service REST" rule rather than a REST call wearing a Kafka costume.
 */
public interface ProviderStatusPort {

    /**
     * @throws com.example.psp.paymentapi.domain.exception.ProviderStatusTimeoutException if no
     *     reply arrives within the configured timeout.
     */
    ProviderStatusResult checkStatus(UUID paymentId, String merchantId);
}
