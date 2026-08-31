package com.example.psp.ledger.domain.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * One Kafka record header, carried as plain bytes - see {@link DlqRecord}'s javadoc for why
 * {@code domain/} holds headers this way rather than {@code org.apache.kafka.common.header.Header}
 * (ADR-0007: domain/ must not know Kafka exists, enforced by
 * {@code architecture.HexagonalArchitectureTest#domainMustNotDependOnKafka}).
 *
 * <p>{@code equals}/{@code hashCode} are overridden by hand: a plain record's generated
 * {@code equals} compares array-typed components by reference (arrays never override
 * {@link Object#equals}), which would make two headers with byte-for-byte identical content
 * compare unequal. That footgun matters here specifically because {@code application}'s
 * {@code ReplayDlqUseCaseTest} asserts on header content, and a future caller comparing replayed
 * headers to the originals deserves the same correctness.
 */
public record DlqHeader(String key, byte[] value) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DlqHeader other)) {
            return false;
        }
        return key.equals(other.key) && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(value));
    }
}
