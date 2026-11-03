package com.pulsepipe.common.pipeline;

import com.pulsepipe.common.model.Event;

import java.util.List;

/**
 * Inbound port implemented by the storage layer. The Kafka consumer hands
 * over a whole polled batch; implementations write it durably (e.g. JDBC
 * batch) before the consumer acknowledges offsets.
 */
public interface EventBatchHandler {

    /**
     * Persist a batch of events. Must be idempotent: replayed batches must not
     * create duplicate rows.
     *
     * @return number of rows actually inserted
     * @throws Exception on transient failure so the consumer can retry
     */
    int handleBatch(List<Event> events) throws Exception;
}
