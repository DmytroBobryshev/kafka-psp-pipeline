package com.example.psp.webhooknotifier.adapters.out.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface MerchantWebhookMongoRepository extends MongoRepository<MerchantWebhookDocument, String> {}
