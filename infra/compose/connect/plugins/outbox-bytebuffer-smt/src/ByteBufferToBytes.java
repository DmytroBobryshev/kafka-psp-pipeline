package com.example.psp.connect.smt;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * M9 Phase 1 - one-purpose glue SMT, chained AFTER io.debezium.transforms.outbox.EventRouter in
 * infra/compose/connect/payment-outbox-connector.json ("outbox,byteBufferFix").
 *
 * <p>THE BUG THIS WORKS AROUND: with the outbox event router's table.expand.json.payload=false
 * (M9 Phase 1 - payload is raw Confluent Avro wire bytes, not JSON to parse), the router forwards
 * the outbox_event.payload BYTEA column's value unchanged as the record's whole value. Debezium's
 * JDBC value conversion represents that BYTEA column as a java.nio.HeapByteBuffer, not a raw
 * byte[] - a well-known Debezium/Kafka-Connect data-model detail. Kafka's own
 * org.apache.kafka.connect.converters.ByteArrayConverter#fromConnectData is strictly
 * {@code instanceof byte[]} and throws DataException("... not compatible with objects of type
 * class java.nio.HeapByteBuffer") on anything else - confirmed empirically against this exact
 * stack (see services/payment-api/README.md's M9 section, "Compromises"). No Debezium or Kafka
 * Connect config flag changes that representation; the community-standard fix is exactly this
 * kind of tiny normalizing SMT.
 *
 * <p>Deliberately minimal: no config, no field targeting - by the time this runs, the record's
 * ENTIRE value already IS the payload bytes (that's what table.expand.json.payload=false means),
 * so unwrapping record.value() is enough. If it's already a byte[] (or null), it passes through
 * unchanged.
 */
public class ByteBufferToBytes<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (!(value instanceof ByteBuffer buffer)) {
            return record;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                bytes,
                record.timestamp(),
                record.headers());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources held.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed.
    }
}
