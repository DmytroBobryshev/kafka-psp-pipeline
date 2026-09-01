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
