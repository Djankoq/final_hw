package ru.example.crawler.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class AsyncDataCollectorTest {
    @Test
    void combinesTransformsAndKeepsOrderAndDuplicatesDespiteFailures() {
        AsyncDataCollector.DataSource names = id -> {
            if (id.equals("bad-name")) throw new IOException("name unavailable");
            return "  " + id + "   company\n ";
        };
        AsyncDataCollector.DataSource ratings = id -> {
            if (id.equals("bad-rating")) throw new IOException("rating unavailable");
            return id.equals("bad-number") ? "not-a-number" : "4.75";
        };
        try (var collector = new AsyncDataCollector(names, ratings, raw -> {
            if (raw.contains("bad-transform")) throw new IOException("enrichment failed");
            return AsyncDataCollector.normalizeName(raw);
        }, 4)) {
            List<String> ids = List.of("B", "bad-rating", "bad-name", "bad-number",
                    "bad-transform", "A", "B");
            var async = collector.fetchAllDataAsync(ids);
            assertEquals(List.of(
                    new AsyncDataCollector.FinalResult("B", "B company", new BigDecimal("4.8")),
                    new AsyncDataCollector.FinalResult("A", "A company", new BigDecimal("4.8")),
                    new AsyncDataCollector.FinalResult("B", "B company", new BigDecimal("4.8"))), async);
            assertEquals(collector.fetchAllDataSync(ids), async);
        }
    }

    @Test
    void bothSourcesForAllIdsRunInParallelOnOwnedDaemonThreads() {
        CountDownLatch allSourcesEntered = new CountDownLatch(4);
        Set<Thread> workers = ConcurrentHashMap.newKeySet();
        AsyncDataCollector.DataSource names = id -> {
            workers.add(Thread.currentThread());
            allSourcesEntered.countDown();
            if (!allSourcesEntered.await(3, TimeUnit.SECONDS)) {
                throw new IOException("Sources were serialized");
            }
            return "  " + id + "  ";
        };
        AsyncDataCollector.DataSource ratings = id -> {
            workers.add(Thread.currentThread());
            allSourcesEntered.countDown();
            if (!allSourcesEntered.await(3, TimeUnit.SECONDS)) {
                throw new IOException("Sources were serialized");
            }
            return "4.44";
        };
        try (var collector = new AsyncDataCollector(names, ratings, raw -> {
            workers.add(Thread.currentThread());
            return AsyncDataCollector.normalizeName(raw);
        }, 4)) {
            var results = collector.fetchAllDataAsync(List.of("A", "B"));
            assertEquals(List.of("A", "B"), results.stream().map(AsyncDataCollector.FinalResult::id).toList());
            assertEquals(4, workers.size());
            assertTrue(workers.stream().allMatch(Thread::isDaemon));
            assertTrue(workers.stream().allMatch(thread -> thread.getName().startsWith("data-worker-")));
        }
        assertTrue(workers.stream().noneMatch(Thread::isAlive), "close должен завершить рабочие потоки");
    }

    @Test
    void waitsForBlockingTransformationBeforeReturningList() throws Exception {
        CountDownLatch transforming = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (var collector = new AsyncDataCollector(id -> " name ", id -> "5", raw -> {
            transforming.countDown();
            if (!release.await(3, TimeUnit.SECONDS)) throw new IOException("transform timeout");
            return AsyncDataCollector.normalizeName(raw);
        }, 2)) {
            var result = caller.submit(() -> collector.fetchAllDataAsync(List.of("A")));
            try {
                assertTrue(transforming.await(3, TimeUnit.SECONDS));
                assertFalse(result.isDone());
            } finally {
                release.countDown();
            }
            assertEquals(List.of(new AsyncDataCollector.FinalResult("A", "name", new BigDecimal("5.0"))),
                    result.get(3, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeAllowsAlreadyStartedPipelineToFinish() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService caller = Executors.newFixedThreadPool(2);
        var collector = new AsyncDataCollector(id -> {
            entered.countDown();
            if (!release.await(3, TimeUnit.SECONDS)) throw new IOException("source timeout");
            return " name ";
        }, id -> "4");
        try {
            var result = caller.submit(() -> collector.fetchAllDataAsync(List.of("A")));
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            var closing = caller.submit(collector::close);
            release.countDown();
            assertEquals(1, result.get(3, TimeUnit.SECONDS).size());
            closing.get(3, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, () -> collector.fetchAllDataAsync(List.of("A")));
        } finally {
            release.countDown();
            collector.close();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void forcedCloseFinishesQueuedResultsAndRestoresInterruptFlag() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        ExecutorService caller = Executors.newFixedThreadPool(2);
        var collector = new AsyncDataCollector(id -> {
            worker.set(Thread.currentThread());
            entered.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("Expected interruption");
            } catch (InterruptedException failure) {
                interrupted.countDown();
                throw failure;
            }
        }, id -> "4", AsyncDataCollector::normalizeName, 1);
        try {
            var result = caller.submit(() -> collector.fetchAllDataAsync(List.of("A", "B", "C")));
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            var closing = caller.submit(() -> {
                Thread.currentThread().interrupt();
                collector.close();
                return Thread.currentThread().isInterrupted();
            });
            assertTrue(closing.get(3, TimeUnit.SECONDS));
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            assertTrue(result.get(3, TimeUnit.SECONDS).isEmpty(), "Удалённые из очереди задачи не должны зависнуть");
            assertFalse(worker.get().isAlive());
        } finally {
            collector.close();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void handlesEmptyInputAllFailuresAndValidatesConfiguration() {
        try (var collector = new AsyncDataCollector(id -> "  ", id -> "6")) {
            assertTrue(collector.fetchAllDataAsync(List.of()).isEmpty());
            assertTrue(collector.fetchAllDataSync(List.of()).isEmpty());
            assertTrue(collector.fetchAllDataAsync(List.of("A", "B")).isEmpty());
            assertThrows(IllegalArgumentException.class, () -> collector.fetchAllDataAsync(List.of(" ")));
            assertThrows(NullPointerException.class, () -> collector.fetchAllDataAsync(null));
        }
        assertThrows(IllegalArgumentException.class, () -> new AsyncDataCollector(
                id -> "name", id -> "4", AsyncDataCollector::normalizeName, 0));
        var collector = new AsyncDataCollector(id -> "name", id -> "4");
        collector.close();
        collector.close();
        assertThrows(IllegalStateException.class, () -> collector.fetchAllDataAsync(List.of("A")));
        assertThrows(IllegalStateException.class, () -> collector.fetchAllDataSync(List.of("A")));
    }
}
