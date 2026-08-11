package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.RecordPaymentStatusAuditBatchUseCase;
import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import com.example.psp.analytics.domain.port.PartialBatchWriteException;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

/**
 * M13's batch listener: {@code @KafkaListener(batch = true)} on {@code
 * payments.payment-status-changed.v1}, entirely independent of the Kafka Streams application
 * above (own consumer group - {@code config.BatchListenerKafkaConfig}'s {@code
 * analytics.batch-listener.group-id} - own committed offsets, own container). Where this
 * genuinely helps: {@code adapters.out.mongo.MongoPaymentStatusAuditRepository} turns the whole
 * batch into ONE bulk Mongo write, so a batch of {@code max.poll.records} events (the batch-size
 * lever - see {@code AnalyticsProperties.BatchListener}) costs one round trip to MongoDB instead
 * of one per record.
 *
 * <h2>The failure-handling difference a single-record listener doesn't have</h2>
 *
 * <p>Every other {@code @KafkaListener} in this codebase (ledger's, webhook-notifier's,
 * psp-connector's) processes one record; an exception fails ONE record, and Spring Kafka's normal
 * error handler/backoff/DLQ machinery decides what happens to it in isolation. A batch listener
 * has no such isolation by default: if this method throws a plain exception, Spring Kafka treats
 * the WHOLE batch as failed and, depending on the configured error handler, either re-delivers
 * every record in it (even the ones that already succeeded downstream) or - worse, with a naive
 * setup - loops forever on the same batch if one record in it is permanently bad. Partial
 * progress needs to be signalled explicitly.
 *
 * <p>This listener signals it with {@link BatchListenerFailedException}, constructed with the
 * FAILED RECORD'S INDEX within the batch: {@code config.BatchListenerKafkaConfig}'s {@code
 * DefaultErrorHandler} recognises this exception type specifically, commits offsets for every
 * record before that index (the repository adapter's ordered bulk write guarantees those really
 * did succeed - see its javadoc), and seeks the consumer back to redeliver only from the failed
 * index onward on the next poll. The index comes from {@link PartialBatchWriteException}, the
 * domain-safe (no {@code org.springframework.kafka} import) exception {@code
 * application.RecordPaymentStatusAuditBatchUseCase}'s port is allowed to throw; THIS class is the
 * only place in the hexagon allowed to know {@code BatchListenerFailedException} exists at all
 * (ArchUnit's {@code domainAndApplicationMustNotDependOnKafkaStreams} rule bans {@code
 * org.springframework.kafka..} from {@code domain/} and {@code application/} just as strictly as
 * it bans Kafka Streams), so the translation happens right here, at the boundary.
 */
@Component
public class PaymentStatusChangedBatchListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedBatchListener.class);

    private final RecordPaymentStatusAuditBatchUseCase useCase;
    private final PaymentStatusChangedAuditMapper mapper;

    public PaymentStatusChangedBatchListener(
            RecordPaymentStatusAuditBatchUseCase useCase, PaymentStatusChangedAuditMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${analytics.kafka.payment-status-changed-topic}",
            containerFactory = "paymentStatusAuditBatchKafkaListenerContainerFactory")
    public void onBatch(List<PaymentStatusChanged> events) {
        log.debug("Batch listener received {} record(s) in one poll", events.size());

        List<PaymentStatusAuditEntry> entries = events.stream().map(mapper::toEntry).toList();

        try {
            useCase.execute(entries);
        } catch (PartialBatchWriteException ex) {
            // See this class's javadoc: the index is what lets the container commit offsets for
            // the (genuinely, per the ordered bulk write) already-applied prefix and redeliver
            // only the rest, instead of redelivering - or worse, getting stuck on - the whole
            // batch.
            throw new BatchListenerFailedException(ex.getMessage(), ex, ex.failedIndex());
        }
    }
}
