package com.example.psp.pspconnector.domain.model;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * One record read back off {@code payments.payment-requested.v1.psp-connector.dlq} by
 * {@code domain.port.DlqReader}, ready for {@code application.ReplayDlqUseCase} to republish
 * byte-for-byte to {@code payments.payment-requested.v1} via {@code domain.port.DlqRepublisher}.
 *
 * <p>Deliberately opaque: {@code value} is the raw Confluent-wire-format bytes exactly as read off
 * the DLQ partition (magic byte + schema id + Avro binary - {@code payments.payment-requested.v1}
 * has been Avro since M9 Phase 1), carried as {@code byte[]} rather than decoded back into the
 * generated {@code com.example.psp.common.events.avro.PaymentRequested} type. Two reasons:
 *
 * <ol>
 *   <li>Replay's whole job is "put the record back on the input topic, unchanged" - there is
 *       nothing to inspect or transform, so a typed decode-then-re-encode round trip would only be
 *       a chance to introduce a bug (a schema evolution mismatch, a lossy field mapping) the
 *       original record never had.
 *   <li>{@code domain/} must not know Avro exists (ADR-0007, enforced by
 *       {@code architecture.HexagonalArchitectureTest#domainMustNotDependOnKafka}, which forbids
 *       {@code ..avro..} in {@code domain/} explicitly) - holding raw bytes here, not the generated
 *       Avro class, is what keeps this a plain Java value.
 * </ol>
 *
 * <p>{@code equals}/{@code hashCode} are overridden by hand for the same reason as
 * {@link DlqHeader} - see that record's javadoc.
 *
 * @param key     the record's Kafka key - the {@code paymentId} string, preserved unchanged on
 *                replay so the republished record keeps its original partition assignment
 *                (ADR-0003).
 * @param value   the record's raw value bytes, unchanged.
 * @param headers the record's headers, unchanged.
 */
public record DlqRecord(String key, byte[] value, List<DlqHeader> headers) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DlqRecord other)) {
            return false;
        }
        return key.equals(other.key) && Arrays.equals(value, other.value) && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(value), headers);
    }
}
