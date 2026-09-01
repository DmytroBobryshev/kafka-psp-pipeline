package com.example.psp.webhooknotifier.config;

import com.example.psp.webhooknotifier.adapters.out.persistence.DeliveryAttemptDocument;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;

@Configuration
public class MongoIndexConfig {

    private static final Logger log = LoggerFactory.getLogger(MongoIndexConfig.class);
    private static final String ATTEMPTED_AT_FIELD = "attemptedAt";
    private static final String TTL_INDEX_NAME = "ttl_attempted_at";

    @Bean
    @ConditionalOnProperty(
            prefix = "webhook-notifier.mongo",
            name = "create-ttl-index-on-startup",
            havingValue = "true",
            matchIfMissing = true)
    public ApplicationRunner deliveryAttemptTtlIndexInitializer(
            MongoTemplate mongoTemplate, WebhookNotifierProperties properties) {
        return (ApplicationArguments args) -> {
            long ttlSeconds = properties.mongo().attemptLogTtlSeconds();
            IndexOperations indexOps = mongoTemplate.indexOps(DeliveryAttemptDocument.class);
            indexOps.ensureIndex(
                    new Index()
                            .on(ATTEMPTED_AT_FIELD, org.springframework.data.domain.Sort.Direction.ASC)
                            .expire(ttlSeconds, TimeUnit.SECONDS)
                            .named(TTL_INDEX_NAME));
            log.info(
                    "Ensured TTL index '{}' on delivery_attempts.{} (expireAfterSeconds={})",
                    TTL_INDEX_NAME,
                    ATTEMPTED_AT_FIELD,
                    ttlSeconds);
        };
    }
}
