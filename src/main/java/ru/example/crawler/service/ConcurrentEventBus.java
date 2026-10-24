package ru.example.crawler.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/** Ограниченная очередь событий и доставка подписчикам в собственном пуле. */
public final class ConcurrentEventBus implements AutoCloseable {
    public record Event(String id, String topic, String message) {
        public Event {
            requireText(id, "ID");
            requireText(topic, "Тема");
            Objects.requireNonNull(message, "Сообщение");
        }
    }

    @FunctionalInterface
    public interface EventHandler {
        void onEvent(Event event) throws Exception;
    }

    @FunctionalInterface
    public interface Subscription extends AutoCloseable {
        @Override
        void close();
    }

    public record DeliveryResult(Event event, int delivered, List<String> errors, boolean cancelled) {
        public DeliveryResult {
            errors = List.copyOf(errors);
        }
    }

    public record Statistics(long accepted, long processed, long cancelled,
                             long delivered, long failedDeliveries) { }

    private record Subscriber(String name, EventHandler handler, long number) { }

    private static final class PendingEvent {
        private final Event event;
        private final List<Subscriber> subscribers;
        private final CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
        private final AtomicBoolean finished = new AtomicBoolean();

        private PendingEvent(Event event, List<Subscriber> subscribers) {
            this.event = event;
            this.subscribers = subscribers;
        }
    }

    private final ArrayBlockingQueue<PendingEvent> queue;
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Subscriber>> subscribers = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<DeliveryResult> journal = new ConcurrentLinkedQueue<>();
    private final Set<PendingEvent> pending = ConcurrentHashMap.newKeySet();
    private final Set<Thread> workers = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor;
    private final Duration shutdownTimeout;
    private final Object lifecycle = new Object();
    private final Object closing = new Object();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder processed = new LongAdder();
    private final LongAdder cancelled = new LongAdder();
    private final LongAdder delivered = new LongAdder();
    private final LongAdder failedDeliveries = new LongAdder();
    private volatile boolean accepting = true;
    private volatile boolean stopping;
    private long subscriberNumber;

    public ConcurrentEventBus(int workerCount, int queueCapacity) {
        this(workerCount, queueCapacity, Duration.ofSeconds(5));
    }

    public ConcurrentEventBus(int workerCount, int queueCapacity, Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "Таймаут закрытия");
        if (workerCount < 1 || queueCapacity < 1 || shutdownTimeout.isNegative() || shutdownTimeout.isZero()) {
            throw new IllegalArgumentException("Число потоков, ёмкость очереди и таймаут должны быть положительными");
        }
        this.shutdownTimeout = shutdownTimeout;
        queue = new ArrayBlockingQueue<>(queueCapacity);
        AtomicInteger threadNumber = new AtomicInteger();
        executor = Executors.newFixedThreadPool(workerCount, task -> {
            Thread thread = new Thread(task, "event-worker-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        for (int i = 0; i < workerCount; i++) {
            executor.execute(this::consume);
        }
    }

    /** Подписка влияет на события, опубликованные после её создания. */
    public Subscription subscribe(String topic, String name, EventHandler handler) {
        requireText(topic, "Тема");
        requireText(name, "Имя подписчика");
        Objects.requireNonNull(handler, "Обработчик");
        Subscriber subscriber;
        synchronized (lifecycle) {
            if (!accepting) throw new IllegalStateException("Система событий закрыта");
            subscriber = new Subscriber(name, handler, ++subscriberNumber);
            subscribers.compute(topic, (key, current) -> {
                var updated = current == null ? new CopyOnWriteArrayList<Subscriber>() : current;
                updated.add(subscriber);
                return updated;
            });
        }
        return () -> subscribers.computeIfPresent(topic, (key, current) -> {
            current.remove(subscriber);
            return current.isEmpty() ? null : current;
        });
    }

    /** Не блокирует производителя; переполнение и закрытие возвращаются как failed future. */
    public CompletableFuture<DeliveryResult> publish(Event event) {
        Objects.requireNonNull(event, "Событие");
        var current = subscribers.get(event.topic());
        // Снимок CopyOnWriteArrayList безопасен при одновременной подписке и отписке.
        PendingEvent request = new PendingEvent(event, current == null ? List.of() : List.copyOf(current));
        synchronized (lifecycle) {
            if (!accepting) {
                return CompletableFuture.failedFuture(new IllegalStateException("Система событий закрыта"));
            }
            pending.add(request);
            if (!queue.offer(request)) {
                pending.remove(request);
                return CompletableFuture.failedFuture(new RejectedExecutionException("Очередь событий заполнена"));
            }
            accepted.increment();
            // Копия не позволяет вызывающему коду завершить внутренний future раньше обработки.
            return request.future.copy();
        }
    }

    /** Снимок журнала; во время обработки порядок соответствует завершению событий. */
    public List<DeliveryResult> journal() {
        return List.copyOf(journal);
    }

    /** Во время работы счётчики могут изменяться; после закрытия значения окончательные. */
    public Statistics statistics() {
        return new Statistics(accepted.sum(), processed.sum(), cancelled.sum(),
                delivered.sum(), failedDeliveries.sum());
    }

    private void consume() {
        workers.add(Thread.currentThread());
        try {
            while (!stopping && (accepting || !queue.isEmpty())) {
                try {
                    PendingEvent request = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) deliver(request);
                } catch (InterruptedException interrupted) {
                    if (stopping) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            workers.remove(Thread.currentThread());
        }
    }

    private void deliver(PendingEvent request) {
        int successful = 0;
        List<String> errors = new ArrayList<>();
        boolean interrupted = false;
        for (Subscriber subscriber : request.subscribers) {
            if (request.finished.get()) return;
            if (stopping) {
                interrupted = true;
                break;
            }
            try {
                subscriber.handler().onEvent(request.event);
                successful++;
            } catch (Exception failure) {
                if (failure instanceof InterruptedException && stopping) {
                    Thread.currentThread().interrupt();
                    interrupted = true;
                    break;
                }
                String error = subscriber.name() + ": " + failure;
                errors.add(error);
                System.out.printf("[%s] [id=%s] ERROR %s%n", Thread.currentThread().getName(), request.event.id(), error);
            }
        }
        finish(request, new DeliveryResult(request.event, successful, errors, interrupted));
    }

    private void finish(PendingEvent request, DeliveryResult result) {
        if (request.finished.compareAndSet(false, true)) {
            journal.add(result);
            if (result.cancelled()) cancelled.increment();
            else processed.increment();
            delivered.add(result.delivered());
            failedDeliveries.add(result.errors().size());
            pending.remove(request);
            // Публикуем результат только после обновления журнала и статистики.
            request.future.complete(result);
        }
    }

    @Override
    public void close() {
        if (workers.contains(Thread.currentThread())) {
            throw new IllegalStateException("close нужно вызывать из потока приложения");
        }
        synchronized (closing) {
            closeFromApplication();
        }
    }

    private void closeFromApplication() {
        synchronized (lifecycle) {
            if (!accepting) return;
            accepting = false;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                stopNow();
                if (!executor.awaitTermination(shutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    System.err.println("Обработчики событий не отреагировали на прерывание");
                }
            }
        } catch (InterruptedException interrupted) {
            stopNow();
            Thread.currentThread().interrupt();
        }
    }

    private void stopNow() {
        stopping = true;
        executor.shutdownNow();
        pending.forEach(request -> finish(request,
                new DeliveryResult(request.event, 0, List.of(), true)));
        queue.clear();
    }

    private static void requireText(String text, String label) {
        Objects.requireNonNull(text, label);
        if (text.isBlank()) throw new IllegalArgumentException(label + " не должно быть пустым");
    }
}
