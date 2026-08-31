package com.example.psp.webhooknotifier.adapters.out.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;

/** Spring Data MongoDB repository for {@link MerchantWebhookDocument}. */
public interface MerchantWebhookMongoRepository extends MongoRepository<MerchantWebhookDocument, String> {}
