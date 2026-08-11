package com.example.psp.analytics.domain.port;

/**
 * Thrown by {@link PaymentStatusAuditRepository#saveAll} when a bulk write applied only a prefix
 * of the batch (M13's batch listener). Pure Java (ADR-0007) - no Mongo/Spring type leaks through
 * this port, even though the adapter behind it (a MongoDB bulk write) is exactly where the
 * information comes from.
 *
 * <p>{@code failedIndex} is the position, within the {@code List} passed to {@code saveAll}, of
 * the first entry the bulk write could not apply. The adapter must guarantee (by using an
 * <b>ordered</b> bulk write) that every entry before {@code failedIndex} genuinely succeeded and
 * nothing at or after it was attempted - that guarantee is what lets
 * {@code adapters.in.kafka.PaymentStatusChangedBatchListener} translate this one-to-one into
 * Spring Kafka's {@code BatchListenerFailedException(message, failedIndex)}, which commits
 * offsets for everything before the index and redelivers from the index onward. An
 * <b>unordered</b> bulk write cannot make this promise (a later entry can succeed while an
 * earlier one fails), so it would silently break that translation - see the repository
 * adapter's javadoc for why ordered mode is not a detail here.
 */
public class PartialBatchWriteException extends RuntimeException {

    private final int failedIndex;

    public PartialBatchWriteException(String message, int failedIndex, Throwable cause) {
        super(message, cause);
        this.failedIndex = failedIndex;
    }

    public int failedIndex() {
        return failedIndex;
    }
}
