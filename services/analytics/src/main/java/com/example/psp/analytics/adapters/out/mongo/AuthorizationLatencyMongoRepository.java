package com.example.psp.analytics.adapters.out.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface AuthorizationLatencyMongoRepository
        extends MongoRepository<AuthorizationLatencyDocument, String> {}
