package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import java.time.Instant;
import java.util.List;

/**
 * Outbound port for the MongoDB projection (M10).
 *
 * <p><b>Why project at all, when the state store already holds the answer.</b> The RocksDB
 * windowed store is bounded by its retention (15 minutes here - see
 * {@code adapters.in.kafka.AnalyticsTopology}), lives on one instance's local disk, and is
 * queryable only while that instance is running and has finished restoring. The projection is the
 * copy that outlives all three: it survives the retention window expiring, a wiped
 * {@code state.dir}, and the process being down.
 *
 * <p><b>The write is outside Kafka's guarantee.</b> Exactly the boundary ledger's M7 README calls
 * "where Kafka EOS ends": even under {@code processing.guarantee=exactly_once_v2}, a Mongo write
 * is not enrolled in the Kafka transaction, so a crash between the write and the offset commit
 * replays it. The projection is made safe the M5 way instead - by being idempotent. The document
 * id is {@link MerchantMetricsWindow#key()} and the write is a whole-document replace, so a
 * replayed window converges rather than accumulating.
 */
public interface MetricsProjectionRepository {

    /** Idempotent upsert of one (merchant, window) result. */
    void save(MerchantMetricsWindow window);

    /**
     * Windows for one merchant whose start is at or after {@code from}, newest first. The
     * durable counterpart to the interactive query in {@link WindowMetricsQueryPort}.
     */
    List<MerchantMetricsWindow> findByMerchantSince(String merchantId, Instant from);
}
