package ru.example.crawler.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Периодический сбор данных: планирование и запросы выполняются в разных пулах. */
public final class PeriodicDataAggregator implements AutoCloseable {
    public record EntityData(String id, double value, Instant fetchedAt) { }

    @FunctionalInterface
    public interface DataSource {
        EntityData fetch(String id) throws Exception;
    }

    private final Supplier<List<String>> identifiers;
    private final DataSource source;
    private final Consumer<EntityData> onSuccess;
    private final Consumer<String> onError;
    private final long periodNanos;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService workers;
    private ScheduledFuture<?> periodicTask;
    private boolean closed;

    public PeriodicDataAggregator(Duration period, int parallelism,
                                  Supplier<List<String>> identifiers, DataSource source,
                                  Consumer<EntityData> onSuccess, Consumer<String> onError) {
        Objects.requireNonNull(period, "period");
        if (period.isZero() || period.isNegative() || parallelism < 1) {
            throw new IllegalArgumentException("Период и число рабочих потоков должны быть положительными");
        }
        this.periodNanos = period.toNanos();
        this.identifiers = Objects.requireNonNull(identifiers, "identifiers");
        this.source = Objects.requireNonNull(source, "source");
        this.onSuccess = Objects.requireNonNull(onSuccess, "onSuccess");
        this.onError = Objects.requireNonNull(onError, "onError");
        scheduler = Executors.newSingleThreadScheduledExecutor();
        workers = Executors.newFixedThreadPool(parallelism);
    }

    /** Первый цикл начинается сразу. Повторный start не создаёт второй таймер. */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Сервис уже закрыт");
        }
        if (periodicTask == null) {
            periodicTask = scheduler.scheduleAtFixedRate(this::aggregateSafely,
                    0, periodNanos, TimeUnit.NANOSECONDS);
        }
    }

    private void aggregateSafely() {
        try {
            List<String> ids = List.copyOf(identifiers.get());
            List<Callable<EntityData>> tasks = new ArrayList<>();
            for (String id : ids) {
                tasks.add(() -> source.fetch(id));
            }
            // Запросы идут параллельно; таймер ждёт окончания цикла, избегая наложения циклов.
            List<Future<EntityData>> results = workers.invokeAll(tasks);
            for (int i = 0; i < results.size(); i++) {
                try {
                    onSuccess.accept(results.get(i).get());
                } catch (ExecutionException failure) {
                    reportError("Ошибка запроса " + ids.get(i) + ": " + failure.getCause());
                } catch (RuntimeException failure) {
                    reportError("Ошибка обработки " + ids.get(i) + ": " + failure);
                }
            }
        } catch (InterruptedException interrupted) {
            // invokeAll отменяет незавершённые запросы при прерывании таймера.
            Thread.currentThread().interrupt();
        } catch (RuntimeException failure) {
            reportError("Ошибка цикла агрегации: " + failure);
        }
    }

    private void reportError(String message) {
        try {
            onError.accept(message);
        } catch (RuntimeException loggingFailure) {
            System.err.println(message + "; ошибка логирования: " + loggingFailure);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        if (periodicTask != null) periodicTask.cancel(true);
        scheduler.shutdownNow();
        workers.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Имитация внешнего источника: задержка 1–3 с, ошибка примерно в 25% запросов. */
    public static EntityData fetchSimulated(String id) throws Exception {
        TimeUnit.SECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 4));
        if (ThreadLocalRandom.current().nextInt(4) == 0) {
            throw new IllegalStateException("Внешний источник недоступен для " + id);
        }
        return new EntityData(id, ThreadLocalRandom.current().nextDouble(50, 150), Instant.now());
    }

    public static void main(String[] args) throws InterruptedException {
        long periodSeconds = args.length > 0 ? Long.parseLong(args[0]) : 5;
        long runSeconds = args.length > 1 ? Long.parseLong(args[1]) : 16;
        if (args.length > 2 || runSeconds < 1) {
            throw new IllegalArgumentException("Использование: [период_секунд длительность_секунд]");
        }
        try (var aggregator = new PeriodicDataAggregator(Duration.ofSeconds(periodSeconds), 5,
                () -> List.of("USD", "EUR", "GBP", "CNY", "TRY"),
                PeriodicDataAggregator::fetchSimulated,
                data -> System.out.printf("Курс %s: %.2f, получен %s%n",
                        data.id(), data.value(), data.fetchedAt()),
                System.err::println)) {
            aggregator.start();
            TimeUnit.SECONDS.sleep(runSeconds);
        }
        System.out.println("Агрегатор остановлен, пулы закрыты.");
    }
}
