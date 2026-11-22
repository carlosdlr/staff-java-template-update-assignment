package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TemplatePublishWorkerTest {
    @Test
    void releasesCapacityPermitBeforeRetryingDownstreamCall() throws Exception {
        var permitHeld = new AtomicBoolean();
        var acquireCalls = new AtomicInteger();
        var downstreamCalls = new AtomicInteger();
        var checkpointed = new AtomicBoolean();

        TemplatePublishWorker.CapacityGate gate = timeout -> {
            assertFalse(permitHeld.get(), "capacity permit must be released before reacquiring");
            assertTrue(permitHeld.compareAndSet(false, true));
            acquireCalls.incrementAndGet();
            return () -> assertTrue(permitHeld.compareAndSet(true, false));
        };
        TemplatePublishWorker.EngagementIndex index = (product, market, cursor, limit) ->
                new TemplatePublishWorker.Page(
                        List.of(new TemplatePublishWorker.EngagementRef("firm-1", "eng-1", "v1")),
                        null);
        TemplatePublishWorker.PendingRefresh downstream = (publish, engagement, key) -> {
            if (downstreamCalls.incrementAndGet() == 1) {
                throw new TemplatePublishWorker.PendingRefreshException("temporary failure");
            }
        };
        TemplatePublishWorker.CheckpointStore checkpoints = new TemplatePublishWorker.CheckpointStore() {
            @Override public String load(String eventId) { return null; }
            @Override public void save(String eventId, String cursor) {}
            @Override public void complete(String eventId) { checkpointed.set(true); }
        };
        TemplatePublishWorker worker = new TemplatePublishWorker(
                index,
                gate,
                downstream,
                checkpoints,
                (publish, engagement, key, reason) -> {
                    throw new AssertionError("successful retry must not be dead-lettered");
                },
                new TemplatePublishWorker.Limits(
                        1, 1, 2, Duration.ofSeconds(1), Duration.ZERO));

        try (worker) {
            worker.process(new TemplatePublishWorker.Publish("event-1", "product-1", "CA", "v2"));
        }

        assertEquals(2, acquireCalls.get());
        assertEquals(2, downstreamCalls.get());
        assertFalse(permitHeld.get());
        assertTrue(checkpointed.get());
    }
}
