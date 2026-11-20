package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Bounded, resumable fan-out. Durable adapters are supplied by the hosting service. */
public final class TemplatePublishWorker implements AutoCloseable {
    public record Publish(String eventId, String productId, String market, String targetVersion) {
        public Publish {
            requireText(eventId, "eventId");
            requireText(productId, "productId");
            requireText(market, "market");
            requireText(targetVersion, "targetVersion");
        }
    }

    public record EngagementRef(String firmId, String engagementId, String appliedVersion) {
        public EngagementRef {
            requireText(firmId, "firmId");
            requireText(engagementId, "engagementId");
            requireText(appliedVersion, "appliedVersion");
        }
    }

    public record Page(List<EngagementRef> items, String nextCursor) {
        public Page {
            items = List.copyOf(items);
        }
    }

    public record Limits(int concurrency, int pageSize, int maxAttempts,
                         Duration permitTimeout, Duration retryBase) {
        public Limits {
            if (concurrency < 1 || pageSize < concurrency || maxAttempts < 1) {
                throw new IllegalArgumentException("invalid worker limits");
            }
            Objects.requireNonNull(permitTimeout, "permitTimeout");
            Objects.requireNonNull(retryBase, "retryBase");
            if (permitTimeout.isNegative() || permitTimeout.isZero() || retryBase.isNegative()) {
                throw new IllegalArgumentException("timeouts must be positive and retryBase non-negative");
            }
        }
    }

    public interface EngagementIndex {
        /** Stable keyset pagination; implementations scope by product/market and cursor. */
        Page findPage(String productId, String market, String cursor, int limit);
    }

    public interface CapacityGate {
        /** Acquire a globally coordinated permit, waiting at most for the supplied duration. */
        Permit acquire(Duration timeout) throws InterruptedException, CapacityUnavailableException;
        interface Permit extends AutoCloseable {
            @Override void close();
        }
    }

    /** Capacity pressure defers the publish event for queue retry; it is not a bad engagement. */
    public static final class CapacityUnavailableException extends Exception {
        private static final long serialVersionUID = 1L;
        public CapacityUnavailableException(String message) { super(message); }
        public CapacityUnavailableException(String message, Throwable cause) { super(message, cause); }
    }

    public interface PendingRefresh {
        /** Idempotent by key; may take about one minute and must not apply template content. */
        void refreshPending(Publish publish, EngagementRef engagement, String idempotencyKey) throws Exception;
    }

    /** A known permanent input or policy failure that should go to the DLQ without retry. */
    public static final class NonRetryableFailure extends Exception {
        private static final long serialVersionUID = 1L;
        public NonRetryableFailure(String message) { super(message); }
        public NonRetryableFailure(String message, Throwable cause) { super(message, cause); }
    }

    public interface CheckpointStore {
        /** Return the last fully completed page cursor, or null to begin. */
        String load(String publishEventId);
        /** Persist only after every item on the page completed or reached the DLQ. */
        void save(String publishEventId, String completedPageCursor);
        void complete(String publishEventId);
    }

    public interface DeadLetter {
        /** Must durably record the failure before returning. */
        void record(Publish publish, EngagementRef engagement, String idempotencyKey, String reason);
    }

    private final EngagementIndex index;
    private final CapacityGate gate;
    private final PendingRefresh downstream;
    private final CheckpointStore checkpoints;
    private final DeadLetter deadLetter;
    private final Limits limits;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public TemplatePublishWorker(EngagementIndex index, CapacityGate gate, PendingRefresh downstream,
                                 CheckpointStore checkpoints, DeadLetter deadLetter, Limits limits) {
        this.index = Objects.requireNonNull(index);
        this.gate = Objects.requireNonNull(gate);
        this.downstream = Objects.requireNonNull(downstream);
        this.checkpoints = Objects.requireNonNull(checkpoints);
        this.deadLetter = Objects.requireNonNull(deadLetter);
        this.limits = Objects.requireNonNull(limits);
    }

    /** Safe to redeliver: page replay uses the same per-item idempotency keys. */
    public void process(Publish publish) throws InterruptedException, CapacityUnavailableException {
        Objects.requireNonNull(publish, "publish");
        var cursor = checkpoints.load(publish.eventId());
        while (true) {
            var page = Objects.requireNonNull(index.findPage(
                    publish.productId(), publish.market(), cursor, limits.pageSize()), "index page");
            if (page.items().isEmpty()) {
                if (page.nextCursor() == null) break;
                if (Objects.equals(cursor, page.nextCursor())) {
                    throw new IllegalStateException("engagement index pagination cursor did not advance");
                }
                cursor = page.nextCursor();
                checkpoints.save(publish.eventId(), cursor);
                continue;
            }

            if (page.nextCursor() != null && Objects.equals(cursor, page.nextCursor())) {
                throw new IllegalStateException("engagement index pagination cursor did not advance");
            }

            processPage(publish, page.items());
            cursor = page.nextCursor();
            if (cursor != null) checkpoints.save(publish.eventId(), cursor);
            if (cursor == null) break;
        }
        checkpoints.complete(publish.eventId());
    }

    private void processPage(Publish publish, List<EngagementRef> refs)
            throws InterruptedException, CapacityUnavailableException {
        var completions = new ExecutorCompletionService<Void>(executor);
        var outstanding = new ArrayList<Future<Void>>();
        var failures = new ArrayList<Throwable>();
        try {
            for (var ref : refs) {
                outstanding.add(completions.submit(() -> {
                    deliver(publish, ref);
                    return null;
                }));
                if (outstanding.size() == limits.concurrency()) {
                    awaitOne(completions, outstanding, failures);
                }
            }
            while (!outstanding.isEmpty()) awaitOne(completions, outstanding, failures);
        } catch (InterruptedException interrupted) {
            outstanding.forEach(task -> task.cancel(true));
            throw interrupted;
        }
        if (!failures.isEmpty()) {
            var failure = failures.get(0);
            if (failure instanceof InterruptedException interrupted) throw interrupted;
            if (failure instanceof CapacityUnavailableException unavailable) throw unavailable;
            throw new IllegalStateException("fan-out page failed before durable completion", failure);
        }
    }

    private void deliver(Publish publish, EngagementRef ref)
            throws InterruptedException, CapacityUnavailableException {
        var key = "%s:%s:%s".formatted(publish.eventId(), ref.firmId(), ref.engagementId());
        Exception lastFailure = null;
        for (var attempt = 1; attempt <= limits.maxAttempts(); attempt++) {
            CapacityGate.Permit permit;
            try {
                permit = Objects.requireNonNull(gate.acquire(limits.permitTimeout()), "capacity gate permit");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (CapacityUnavailableException unavailable) {
                throw unavailable;
            }
            try (permit) {
                downstream.refreshPending(publish, ref, key);
                return;
            } catch (NonRetryableFailure permanent) {
                deadLetter.record(publish, ref, key, permanent.toString());
                return;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (Exception failure) {
                lastFailure = failure;
                if (attempt < limits.maxAttempts()) {
                    Thread.sleep(backoff(attempt).toMillis());
                }
            }
        }
        deadLetter.record(publish, ref, key,
                lastFailure == null ? "unknown failure" : lastFailure.toString());
    }

    private Duration backoff(int failedAttempt) {
        var multiplier = 1L << Math.min(failedAttempt - 1, 20);
        var capMillis = TimeUnit.MINUTES.toMillis(5);
        var baseMillis = limits.retryBase().compareTo(Duration.ofMillis(capMillis)) > 0
                ? capMillis : limits.retryBase().toMillis();
        var ceilingMillis = baseMillis > capMillis / multiplier ? capMillis : baseMillis * multiplier;
        var jitterMillis = ceilingMillis == 0 ? 0 : ThreadLocalRandom.current().nextLong(ceilingMillis + 1);
        return Duration.ofMillis(jitterMillis);
    }

    private static void awaitOne(ExecutorCompletionService<Void> completions, List<Future<Void>> outstanding,
                                 List<Throwable> failures) throws InterruptedException {
        var completed = completions.take();
        outstanding.remove(completed);
        try {
            completed.get();
        } catch (ExecutionException failure) {
            failures.add(failure.getCause());
        } catch (java.util.concurrent.CancellationException failure) {
            failures.add(failure);
        }
    }

    @Override public void close() {
        executor.close();
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
    }
}
