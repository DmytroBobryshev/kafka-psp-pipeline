package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import java.util.List;

/**
 * Outbound port for M17 page 3's generic DLQ browse: a non-destructive peek at the LAST
 * {@code max} records currently sitting on a {@code *.dlq} topic. Backed by
 * {@code adapters.out.kafka.KafkaDlqBrowser} - a short-lived, group-less {@code KafkaConsumer}
 * that {@code assign()}s every partition directly and never commits an offset, so calling this
 * repeatedly never "drains" the DLQ the way {@code webhook-notifier}'s {@code DlqReader} (a real
 * consumer-group reader used for replay) does.
 */
public interface DlqBrowser {

    /**
     * @param topic a topic name ending in {@code .dlq} - {@code application.BrowseDlqUseCase} has
     *              already rejected anything else before this is ever called.
     * @param max   how many of the most recent records to return, already clamped by the caller.
     */
    List<DlqRecordView> peekLast(String topic, int max);
}
