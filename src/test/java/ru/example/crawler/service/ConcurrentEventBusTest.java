package ru.example.crawler.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.example.crawler.service.ConcurrentEventBus.Event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class ConcurrentEventBusTest {
    @Test
    void concurrentProducersDeliverEveryEventToEverySubscriber() throws Exception {
        var notifications = new ConcurrentHashMap<String, AtomicInteger>();
        var audit = new ConcurrentHashMap<String, AtomicInteger>();
        var deliveries = new CopyOnWriteArrayList<CompletableFuture<ConcurrentEventBus.DeliveryResult>>();
        CountDownLatch start = new CountDownLatch(1);
        var producers = Executors.newFixedThreadPool(4);
        try (var bus = new ConcurrentEventBus(3, 256)) {
            bus.subscribe("updates", "notifications", event ->
                    notifications.computeIfAbsent(event.id(), key -> new AtomicInteger()).incrementAndGet());
            bus.subscribe("updates", "audit", event ->
                    audit.computeIfAbsent(event.id(), key -> new AtomicInteger()).incrementAndGet());
            List<Future<?>> publications = new ArrayList<>();
            for (int producer = 0; producer < 4; producer++) {
                int number = producer;
                publications.add(producers.submit(() -> {
                    start.await();
                    for (int i = 0; i < 50; i++) {
                        deliveries.add(bus.publish(event(number + "-" + i)));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> publication : publications) publication.get(5, TimeUnit.SECONDS);
            CompletableFuture.allOf(deliveries.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
            assertEquals(200, notifications.size());
            assertEquals(notifications.keySet(), audit.keySet());
            assertTrue(notifications.values().stream().allMatch(count -> count.get() == 1));
            assertTrue(audit.values().stream().allMatch(count -> count.get() == 1));
            assertEquals(200, bus.journal().size());
            assertEquals(200, bus.journal().stream().map(result -> result.event().id()).distinct().count());
            assertEquals(new ConcurrentEventBus.Statistics(200, 200, 0, 400, 0), bus.statistics());
        } finally {
            start.countDown();
            producers.shutdownNow();
            assertTrue(producers.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void subscriberFailureDoesNotPreventOthersAndTopicsAreIndependent() throws Exception {
        AtomicInteger successful = new AtomicInteger();
        try (var bus = new ConcurrentEventBus(2, 8)) {
            bus.subscribe("updates", "failing", event -> { throw new IllegalStateException("unavailable"); });
            bus.subscribe("updates", "healthy", event -> successful.incrementAndGet());
            bus.subscribe("other", "unrelated", event -> fail("Wrong topic"));
            var result = bus.publish(event("A")).get(3, TimeUnit.SECONDS);
            assertEquals(1, result.delivered());
            assertEquals(1, result.errors().size());
            assertTrue(result.errors().get(0).contains("failing"));
            assertFalse(result.cancelled());
            var noSubscribers = bus.publish(new Event("B", "unknown", "message")).get(3, TimeUnit.SECONDS);
            assertEquals(0, noSubscribers.delivered());
            assertTrue(noSubscribers.errors().isEmpty());
            assertEquals(1, successful.get());
            assertEquals(new ConcurrentEventBus.Statistics(2, 2, 0, 1, 1), bus.statistics());
        }
    }

    @Test
    void publicationKeepsSubscriberSnapshotDuringSubscriptionChanges() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var oldSubscriber = new CopyOnWriteArrayList<String>();
        var newSubscriber = new CopyOnWriteArrayList<String>();
        try (var bus = new ConcurrentEventBus(1, 8)) {
            bus.subscribe("updates", "blocking", event -> {
                entered.countDown();
                if (!release.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("timeout");
            });
            var subscription = bus.subscribe("updates", "old", event -> oldSubscriber.add(event.id()));
            var first = bus.publish(event("first"));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                subscription.close();
                subscription.close();
                bus.subscribe("updates", "new", event -> newSubscriber.add(event.id()));
                var second = bus.publish(event("second"));
                release.countDown();
                assertEquals(2, first.get(3, TimeUnit.SECONDS).delivered());
                assertEquals(2, second.get(3, TimeUnit.SECONDS).delivered());
                assertEquals(List.of("first"), oldSubscriber);
                assertEquals(List.of("second"), newSubscriber);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void fullQueueRejectsNewEventWithoutLosingAcceptedEvents() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var bus = new ConcurrentEventBus(1, 1)) {
            bus.subscribe("updates", "blocking", event -> {
                entered.countDown();
                if (!release.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("timeout");
            });
            var active = bus.publish(event("active"));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                var queued = bus.publish(event("queued"));
                var rejected = bus.publish(event("rejected"));
                var failure = assertThrows(CompletionException.class, rejected::join);
                assertInstanceOf(RejectedExecutionException.class, failure.getCause());
                release.countDown();
                assertEquals(1, active.get(3, TimeUnit.SECONDS).delivered());
                assertEquals(1, queued.get(3, TimeUnit.SECONDS).delivered());
                assertEquals(new ConcurrentEventBus.Statistics(2, 2, 0, 2, 0), bus.statistics());
                assertEquals(2, bus.journal().size());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void closeDrainsQueueAndStopsOwnedDaemonThreads() throws Exception {
        Set<Thread> workers = ConcurrentHashMap.newKeySet();
        var bus = new ConcurrentEventBus(2, 16);
        try {
            bus.subscribe("updates", "observer", event -> workers.add(Thread.currentThread()));
            List<CompletableFuture<ConcurrentEventBus.DeliveryResult>> deliveries = new ArrayList<>();
            for (int i = 0; i < 10; i++) deliveries.add(bus.publish(event("event-" + i)));
            bus.close();
            for (var delivery : deliveries) {
                assertEquals(1, delivery.get(3, TimeUnit.SECONDS).delivered());
                assertFalse(delivery.join().cancelled());
            }
            assertFalse(workers.isEmpty());
            assertTrue(workers.stream().allMatch(Thread::isDaemon));
            assertTrue(workers.stream().allMatch(thread -> thread.getName().startsWith("event-worker-")));
            assertTrue(workers.stream().noneMatch(Thread::isAlive));
            assertEquals(10, bus.statistics().processed());
            assertThrows(CompletionException.class, () -> bus.publish(event("late")).join());
            assertThrows(IllegalStateException.class, () -> bus.subscribe("updates", "late", event -> { }));
            bus.close();
        } finally {
            bus.close();
        }
    }

    @Test
    void forcedCloseInterruptsHandlerAndCompletesQueuedFutures() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        var bus = new ConcurrentEventBus(1, 4, Duration.ofMillis(100));
        try {
            bus.subscribe("updates", "blocking", event -> {
                worker.set(Thread.currentThread());
                entered.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException failure) {
                    interrupted.countDown();
                    throw failure;
                }
            });
            var active = bus.publish(event("active"));
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            var queued = bus.publish(event("queued"));
            bus.close();
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            assertTrue(active.get(3, TimeUnit.SECONDS).cancelled());
            assertTrue(queued.get(3, TimeUnit.SECONDS).cancelled());
            assertFalse(worker.get().isAlive());
            assertEquals(new ConcurrentEventBus.Statistics(2, 0, 2, 0, 0), bus.statistics());
            assertEquals(2, bus.journal().size());
        } finally {
            bus.close();
        }
    }

    @Test
    void handlerInterruptionIsIsolatedWhileBusIsRunning() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger successful = new AtomicInteger();
        try (var bus = new ConcurrentEventBus(1, 4)) {
            bus.subscribe("updates", "interrupted", event -> {
                if (calls.incrementAndGet() == 1) throw new InterruptedException("source interrupted");
            });
            bus.subscribe("updates", "healthy", event -> successful.incrementAndGet());
            var first = bus.publish(event("A")).get(3, TimeUnit.SECONDS);
            var second = bus.publish(event("B")).get(3, TimeUnit.SECONDS);
            assertEquals(1, first.delivered());
            assertEquals(1, first.errors().size());
            assertFalse(first.cancelled());
            assertEquals(2, second.delivered());
            assertEquals(2, successful.get());
        }
    }

    @Test
    void validatesInputAndRejectsCloseFromHandler() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentEventBus(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentEventBus(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentEventBus(1, 1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new Event(" ", "topic", "message"));
        assertThrows(NullPointerException.class, () -> new Event("id", "topic", null));
        try (var bus = new ConcurrentEventBus(1, 4)) {
            assertThrows(IllegalArgumentException.class, () -> bus.subscribe(" ", "name", event -> { }));
            assertThrows(NullPointerException.class, () -> bus.publish(null));
            bus.subscribe("updates", "closing", event -> bus.close());
            var result = bus.publish(event("A")).get(3, TimeUnit.SECONDS);
            assertEquals(1, result.errors().size());
            assertTrue(result.errors().get(0).contains("close"));
        }
    }

    private static Event event(String id) {
        return new Event(id, "updates", "Organization updated");
    }
}
