package com.example.psp.analytics.adapters.out.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Spring Data repository for the {@code authorization_latency} collection (M13). A framework
 * interface, so it lives in {@code adapters/out} and is never referenced from {@code
 * application/} or {@code domain/} - {@code domain.port.AuthorizationLatencyProjectionRepository}
 * is what those layers see (ADR-0007). No query methods beyond CRUD are needed yet; {@code
 * save()}'s upsert-by-id behaviour is the entire projection story - see the document's javadoc.
 */
public interface AuthorizationLatencyMongoRepository
        extends MongoRepository<AuthorizationLatencyDocument, String> {}
