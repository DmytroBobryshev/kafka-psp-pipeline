/**
 * Spring configuration, bean wiring, and profiles (ADR-0007).
 *
 * <p>M7's two halves live here and are worth reading in this order:
 * {@link com.example.psp.ledger.config.KafkaProducerConfig} (transactional producer,
 * {@code transactional.id} and zombie fencing, the transaction coordinator and its commit/abort
 * markers, plus the two transaction managers this service needs) and
 * {@link com.example.psp.ledger.config.KafkaConsumerConfig} ({@code isolation.level=read_committed},
 * and the one line that turns "produce inside a transaction" into full consume-process-produce
 * exactly-once by putting the consumed offsets in the same transaction).
 */
package com.example.psp.ledger.config;
