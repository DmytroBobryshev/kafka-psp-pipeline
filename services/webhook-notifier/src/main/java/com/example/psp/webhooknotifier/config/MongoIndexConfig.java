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

/**
 * Creates the M8 TTL index on {@code delivery_attempts.attemptedAt} once at startup, with a
 * runtime-configurable duration ({@code webhook-notifier.mongo.attempt-log-ttl-seconds}) - not a
 * static {@code @Indexed(expireAfterSeconds=...)} annotation, which requires a compile-time
 * constant and could not be driven by a property. {@code IndexOperations#ensureIndex} is
 * idempotent (a no-op if an identically-named, identically-defined index already exists), so
 * this runs safely on every application start, not just the first.
 *
 * <p><b>Known limitation:</b> if the TTL property value CHANGES between deploys, MongoDB rejects
 * re-creating an index with the same keys but a different {@code expireAfterSeconds} (an
 * {@code IndexOptionsConflict} error) rather than updating it in place - this runner does not
 * attempt to detect or {@code collMod} an existing index's TTL, so changing the property on an
 * already-running database requires dropping the index manually first. Acceptable for this
 * learning system; a production migration tool would use {@code collMod} explicitly.
 */
@Configuration
public class MongoIndexConfig {

    private static final Logger log = LoggerFactory.getLogger(MongoIndexConfig.class);
    private static final String ATTEMPTED_AT_FIELD = "attemptedAt";
    private static final String TTL_INDEX_NAME = "ttl_attempted_at";

    /**
     * Gated by {@code webhook-notifier.mongo.create-ttl-index-on-startup} (default {@code true})
     * rather than always running unconditionally: this runner performs a REAL MongoDB round trip
     * at startup (unlike the lazily-connecting {@code MongoTemplate}/{@code MongoClient} beans
     * Spring Boot autoconfigures), so {@code WebhookNotifierApplicationTests} - which loads the
     * full application context against an embedded Kafka broker but no live MongoDB, the same
     * "embedded broker stands in for compose" pattern every other service's context-load test
     * uses - sets this to {@code false} to avoid requiring a real database just to prove the
     * context wires up.
     */
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
