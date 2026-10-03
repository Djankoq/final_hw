package ru.example.crawler.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class PeriodicDataAggregatorTest {
    @Test
    void requestsRunInParallelAndFailureDoesNotDiscardSuccessfulResults() throws Exception {
        CountDownLatch entered = new CountDownLatch(3);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch processed = new CountDownLatch(3);
        var successes = new CopyOnWriteArrayList<String>();
        var errors = new CopyOnWriteArrayList<String>();
        try (var aggregator = new PeriodicDataAggregator(Duration.ofHours(1), 3,
                () -> List.of("USD", "EUR", "GBP"), id -> {
                    entered.countDown();
                    if (!release.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("timeout");
                    if (id.equals("EUR")) throw new IllegalStateException("unavailable");
                    return new PeriodicDataAggregator.EntityData(id, 100, Instant.now());
                }, data -> {
                    successes.add(data.id());
                    processed.countDown();
                }, error -> {
                    errors.add(error);
                    processed.countDown();
                })) {
            aggregator.start();
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS), "Все запросы должны начаться параллельно");
            } finally {
                release.countDown();
            }
            assertTrue(processed.await(3, TimeUnit.SECONDS));
            assertEquals(List.of("USD", "GBP"), successes);
            assertEquals(1, errors.size());
            assertTrue(errors.get(0).contains("EUR"));
        }
    }

    @Test
    void nextCycleRunsAfterIdentifierSupplierFails() throws Exception {
        AtomicInteger cycles = new AtomicInteger();
        CountDownLatch recovered = new CountDownLatch(1);
        var errors = new CopyOnWriteArrayList<String>();
        try (var aggregator = new PeriodicDataAggregator(Duration.ofMillis(20), 1, () -> {
            if (cycles.incrementAndGet() == 1) throw new IllegalStateException("identifier failure");
            return List.of("USD");
        }, id -> new PeriodicDataAggregator.EntityData(id, 100, Instant.now()),
                data -> recovered.countDown(), errors::add)) {
            aggregator.start();
            assertTrue(recovered.await(3, TimeUnit.SECONDS));
            assertTrue(cycles.get() >= 2);
            assertTrue(errors.get(0).contains("identifier failure"));
        }
    }

    @Test
    void resultHandlerFailureDoesNotPreventProcessingOtherResults() throws Exception {
        CountDownLatch success = new CountDownLatch(1);
        var errors = new CopyOnWriteArrayList<String>();
        try (var aggregator = new PeriodicDataAggregator(Duration.ofHours(1), 2,
                () -> List.of("USD", "EUR"),
                id -> new PeriodicDataAggregator.EntityData(id, 100, Instant.now()), data -> {
                    if (data.id().equals("USD")) throw new IllegalStateException("storage failure");
                    success.countDown();
                }, errors::add)) {
            aggregator.start();
            assertTrue(success.await(3, TimeUnit.SECONDS));
            assertEquals(1, errors.size());
            assertTrue(errors.get(0).contains("storage failure"));
        }
    }

    @Test
    void closeInterruptsInFlightRequestAndPreventsRestart() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        var aggregator = new PeriodicDataAggregator(Duration.ofHours(1), 1,
                () -> List.of("USD"), id -> {
                    entered.countDown();
                    try {
                        new CountDownLatch(1).await();
                        throw new AssertionError("Запрос должен быть прерван");
                    } catch (InterruptedException failure) {
                        interrupted.countDown();
                        throw failure;
                    }
                }, data -> processed.incrementAndGet(), error -> { });
        try {
            aggregator.start();
            aggregator.start();
            assertTrue(entered.await(3, TimeUnit.SECONDS));
        } finally {
            aggregator.close();
        }
        assertTrue(interrupted.await(3, TimeUnit.SECONDS));
        assertEquals(0, processed.get());
        assertThrows(IllegalStateException.class, aggregator::start);
        aggregator.close();
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new PeriodicDataAggregator(
                Duration.ZERO, 1, List::of, PeriodicDataAggregator::fetchSimulated,
                data -> { }, error -> { }));
        assertThrows(IllegalArgumentException.class, () -> new PeriodicDataAggregator(
                Duration.ofSeconds(1), 0, List::of, PeriodicDataAggregator::fetchSimulated,
                data -> { }, error -> { }));
    }
}
