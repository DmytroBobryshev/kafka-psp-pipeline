package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import com.example.psp.analytics.domain.port.PartialBatchWriteException;
import com.example.psp.analytics.domain.port.PaymentStatusAuditRepository;
import com.mongodb.bulk.BulkWriteError;
import java.util.Comparator;
import java.util.List;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

/**
 * MongoDB adapter for {@link PaymentStatusAuditRepository} (M13) - the bulk-write half of the
 * batch listener's whole point.
 *
 * <h2>One bulk write, not N single writes</h2>
 *
 * <p>{@link #saveAll} builds ONE {@link BulkOperations} covering the whole batch and calls {@code
 * execute()} once, issuing a single {@code bulkWrite} command to MongoDB regardless of whether
 * the batch holds 1 record or {@code max.poll.records} of them - the entire "N round trips become
 * one" story {@code adapters.in.kafka.PaymentStatusChangedBatchListener}'s javadoc promises.
 *
 * <h2>{@code replaceOne(..., upsert())}, not {@code insert(...)}</h2>
 *
 * <p>Each entry is upserted by its {@code _id} ({@code envelope.eventId}), not inserted. This
 * matters for more than idempotency: a plain {@code insert} of an already-processed batch
 * (redelivered after a crash before the offset commit, or replayed by a rebalance) would hit a
 * duplicate-key error on every entry that already succeeded - which is not a real failure, it is
 * proof the entry was already durably written. Translating THAT into
 * {@link PartialBatchWriteException} would tell the listener to redeliver a record that already
 * succeeded, which would hit the same duplicate-key error again, forever. Upserting makes a
 * redelivered entry a harmless no-op overwrite instead, so the only way {@link
 * #saveAll} can legitimately fail here is a genuine write error unrelated to redelivery (a
 * document too large, a validator rejection, a network fault mid-batch) - exactly the case {@link
 * PartialBatchWriteException} exists for.
 *
 * <h2>{@code BulkMode.ORDERED}, not {@code UNORDERED}</h2>
 *
 * <p>Ordered mode stops at the first failing operation and does not attempt anything after it -
 * the guarantee {@link PartialBatchWriteException}'s javadoc requires to safely tell Spring Kafka
 * "everything before this index is done, redeliver from here". Unordered mode would let a LATER
 * entry succeed while an EARLIER one fails, which would make "redeliver from the failed index
 * onward" unsound (a successfully-applied earlier entry could still get redelivered, or worse, an
 * unresolved earlier failure could get skipped as if it were fine).
 */
@Component
public class MongoPaymentStatusAuditRepository implements PaymentStatusAuditRepository {

    private static final String COLLECTION = "payment_status_audit";

    private final MongoTemplate mongoTemplate;
    private final PaymentStatusAuditMapper mapper;

    public MongoPaymentStatusAuditRepository(MongoTemplate mongoTemplate, PaymentStatusAuditMapper mapper) {
        this.mongoTemplate = mongoTemplate;
        this.mapper = mapper;
    }

    @Override
    public void saveAll(List<PaymentStatusAuditEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        BulkOperations bulkOps =
                mongoTemplate.bulkOps(
                        BulkOperations.BulkMode.ORDERED, PaymentStatusAuditDocument.class, COLLECTION);

        for (PaymentStatusAuditEntry entry : entries) {
            PaymentStatusAuditDocument document = mapper.toDocument(entry);
            bulkOps.replaceOne(
                    Query.query(Criteria.where("_id").is(document.getId())),
                    document,
                    FindAndReplaceOptions.options().upsert());
        }

        try {
            bulkOps.execute();
        } catch (BulkOperationException ex) {
            int failedIndex =
                    ex.getErrors().stream()
                            .map(BulkWriteError::getIndex)
                            .min(Comparator.naturalOrder())
                            .orElse(0);
            throw new PartialBatchWriteException(
                    "bulk upsert into " + COLLECTION + " failed at batch index " + failedIndex
                            + " of " + entries.size(),
                    failedIndex,
                    ex);
        }
    }
}
