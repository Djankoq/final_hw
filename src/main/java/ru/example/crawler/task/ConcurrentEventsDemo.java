package ru.example.crawler.task;

import ru.example.crawler.service.ConcurrentEventBus;
import ru.example.crawler.service.ConcurrentEventBus.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Несколько производителей публикуют события, подписчики получают уведомления. */
public final class ConcurrentEventsDemo {
    private ConcurrentEventsDemo() { }

    public static void main(String[] args) throws Exception {
        var bus = new ConcurrentEventBus(3, 32);
        Thread shutdownHook = new Thread(bus::close, "events-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try (bus) {
            bus.subscribe("organization", "notifications", event ->
                    System.out.printf("[%s] [id=%s] notification: %s%n",
                            Thread.currentThread().getName(), event.id(), event.message()));
            bus.subscribe("organization", "audit", event -> {
                if (event.id().equals("event-7")) {
                    throw new IllegalStateException("Имитируемый сбой аудита");
                }
            });

            List<CompletableFuture<ConcurrentEventBus.DeliveryResult>> deliveries = new CopyOnWriteArrayList<>();
            ExecutorService producers = Executors.newFixedThreadPool(2);
            try {
                List<Future<?>> publications = List.of(
                        producers.submit(() -> publishRange(bus, deliveries, 1, 6)),
                        producers.submit(() -> publishRange(bus, deliveries, 6, 11)));
                for (Future<?> publication : publications) publication.get(5, TimeUnit.SECONDS);
                CompletableFuture.allOf(deliveries.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
            } finally {
                producers.shutdownNow();
                if (!producers.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Потоки производителей не завершились");
                }
            }
        } finally {
            bus.close();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException shutdownInProgress) {
                // При завершении JVM hook уже закрывает систему событий.
            }
        }
        var statistics = bus.statistics();
        if (statistics.accepted() != 10 || statistics.processed() != 10 || statistics.cancelled() != 0
                || statistics.delivered() != 19 || statistics.failedDeliveries() != 1 || bus.journal().size() != 10) {
            throw new IllegalStateException("Некорректная доставка событий: " + statistics);
        }
        System.out.println("Events: " + statistics);
        System.out.println("Journal entries: " + bus.journal().size());
    }

    private static void publishRange(ConcurrentEventBus bus,
                                     List<CompletableFuture<ConcurrentEventBus.DeliveryResult>> deliveries,
                                     int from, int to) {
        for (int i = from; i < to; i++) {
            deliveries.add(bus.publish(new Event("event-" + i, "organization", "Organization updated " + i)));
        }
    }
}
