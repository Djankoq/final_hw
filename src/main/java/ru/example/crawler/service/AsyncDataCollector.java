package ru.example.crawler.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** Получает название и рейтинг организации из двух независимых источников. */
public final class AsyncDataCollector implements AutoCloseable {
    private static final int DEFAULT_POOL_SIZE = Math.max(2,
            Math.min(8, Runtime.getRuntime().availableProcessors()));

    @FunctionalInterface
    public interface DataSource {
        String fetch(String id) throws Exception;
    }

    @FunctionalInterface
    public interface NameTransformer {
        String transform(String name) throws Exception;
    }

    public record FinalResult(String id, String name, BigDecimal rating) { }

    private final DataSource nameSource;
    private final DataSource ratingSource;
    private final NameTransformer nameTransformer;
    private final ExecutorService executor;
    private final Object lifecycle = new Object();
    private final ConcurrentHashMap<CompletableFuture<FinalResult>, String> pending = new ConcurrentHashMap<>();
    private boolean closed;

    public AsyncDataCollector(DataSource nameSource, DataSource ratingSource) {
        this(nameSource, ratingSource, AsyncDataCollector::normalizeName, DEFAULT_POOL_SIZE);
    }

    public AsyncDataCollector(DataSource nameSource, DataSource ratingSource,
                              NameTransformer nameTransformer, int poolSize) {
        this.nameSource = Objects.requireNonNull(nameSource);
        this.ratingSource = Objects.requireNonNull(ratingSource);
        this.nameTransformer = Objects.requireNonNull(nameTransformer);
        if (poolSize < 1) {
            throw new IllegalArgumentException("Размер пула должен быть положительным");
        }
        AtomicInteger threadNumber = new AtomicInteger();
        executor = Executors.newFixedThreadPool(poolSize, task -> {
            Thread thread = new Thread(task, "data-worker-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Все запросы запускаются до ожидания. Метод блокируется до сборки списка,
     * сохраняет порядок и повторы ID, исключая только неудачные результаты.
     * Вызывать из потока приложения, а не из рабочих задач этого же пула.
     */
    public List<FinalResult> fetchAllDataAsync(List<String> ids) {
        List<String> inputs = validateIds(ids);
        FinalResult[] results = new FinalResult[inputs.size()];
        List<CompletableFuture<Void>> completions = new ArrayList<>();
        synchronized (lifecycle) {
            ensureOpen();
            for (int i = 0; i < inputs.size(); i++) {
                String id = inputs.get(i);
                int index = i;
                CompletableFuture<String> name = CompletableFuture
                        .supplyAsync(() -> call(id, "name-source", () -> nameSource.fetch(id)), executor)
                        .thenApplyAsync(raw -> call(id, "name-transform",
                                () -> nameTransformer.transform(raw)), executor);
                CompletableFuture<BigDecimal> rating = CompletableFuture
                        .supplyAsync(() -> call(id, "rating-source", () -> ratingSource.fetch(id)), executor)
                        .thenApply(raw -> call(id, "rating-transform", () -> parseRating(raw)));

                CompletableFuture<FinalResult> result = name
                        .thenCombine(rating, (value, score) -> new FinalResult(id, value, score))
                        .handle((value, failure) -> {
                            if (failure != null) {
                                logFailure(id, failure);
                                return null;
                            }
                            return value;
                        });
                pending.put(result, id);
                result.whenComplete((value, failure) -> pending.remove(result));
                // У каждого запроса свой слот: общий ArrayList из потоков не изменяется.
                completions.add(result.thenAccept(value -> {
                    results[index] = value;
                    if (value != null) {
                        log(id, "accepted " + value);
                    }
                }));
            }
        }
        // Ждём также thenAccept, чтобы все записи в массив завершились до чтения.
        CompletableFuture.allOf(completions.toArray(CompletableFuture[]::new)).join();
        return Arrays.stream(results).filter(Objects::nonNull).toList();
    }

    /** Та же загрузка, преобразования и политика ошибок, последовательно. */
    public List<FinalResult> fetchAllDataSync(List<String> ids) {
        List<String> inputs = validateIds(ids);
        synchronized (lifecycle) {
            ensureOpen();
        }
        List<FinalResult> results = new ArrayList<>();
        for (String id : inputs) {
            try {
                String rawName = call(id, "name-source", () -> nameSource.fetch(id));
                String name = call(id, "name-transform", () -> nameTransformer.transform(rawName));
                String rawRating = call(id, "rating-source", () -> ratingSource.fetch(id));
                BigDecimal rating = call(id, "rating-transform", () -> parseRating(rawRating));
                FinalResult result = new FinalResult(id, name, rating);
                results.add(result);
                log(id, "accepted " + result);
            } catch (RuntimeException failure) {
                logFailure(id, failure);
            }
        }
        return List.copyOf(results);
    }

    public static String normalizeName(String raw) {
        String name = Objects.requireNonNull(raw).strip().replaceAll("\\s+", " ");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Пустое название организации");
        }
        return name;
    }

    private static BigDecimal parseRating(String raw) {
        BigDecimal rating = new BigDecimal(raw.strip());
        if (rating.signum() < 0 || rating.compareTo(BigDecimal.valueOf(5)) > 0) {
            throw new IllegalArgumentException("Рейтинг вне диапазона 0–5: " + raw);
        }
        return rating.setScale(1, RoundingMode.HALF_UP);
    }

    private static List<String> validateIds(List<String> ids) {
        List<String> inputs = List.copyOf(ids);
        if (inputs.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("ID не должен быть пустым");
        }
        return inputs;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Сборщик данных закрыт");
        }
    }

    private static <T> T call(String id, String stage, Callable<T> action) {
        log(id, stage + " started");
        try {
            T value = action.call();
            log(id, stage + " finished");
            return value;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CompletionException(interrupted);
        } catch (Exception failure) {
            throw new CompletionException(failure);
        }
    }

    private static void logFailure(String id, Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        log(id, "ERROR, skipped: " + failure);
    }

    private static void log(String id, String message) {
        System.out.printf("[%s] [id=%s] %s%n", Thread.currentThread().getName(), id, message);
    }

    @Override
    public synchronized void close() {
        CompletableFuture<?>[] active;
        synchronized (lifecycle) {
            if (closed) return;
            closed = true;
            active = pending.keySet().toArray(CompletableFuture[]::new);
        }
        boolean interrupted = false;
        try {
            // Пока конвейеры завершаются, пул ещё принимает их thenApplyAsync.
            CompletableFuture.allOf(active).get(5, TimeUnit.SECONDS);
            executor.shutdown();
        } catch (InterruptedException failure) {
            interrupted = true;
            stopNow();
        } catch (ExecutionException | TimeoutException failure) {
            stopNow();
        }
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                stopNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log("shutdown", "Рабочие задачи не отреагировали на прерывание");
                }
            }
        } catch (InterruptedException failure) {
            interrupted = true;
            stopNow();
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private void stopNow() {
        executor.shutdownNow();
        // supplyAsync из удалённой очереди сам не завершит свой CompletableFuture.
        // Завершаем итоговые стадии, чтобы вызывающий поток не завис в allOf().join().
        pending.forEach((result, id) -> {
            if (result.complete(null)) {
                log(id, "ERROR, skipped during shutdown");
            }
        });
    }

    private static String readProperty(String fileName, String id) throws Exception {
        Thread.sleep(200); // Одинаковая имитация задержки источника для обоих режимов.
        InputStream resource = AsyncDataCollector.class.getResourceAsStream("/data/" + fileName);
        if (resource == null) {
            // Позволяет запускать Java source-file без Maven и classpath ресурсов.
            resource = Files.newInputStream(Path.of("src/main/resources/data", fileName));
        }
        Properties properties = new Properties();
        try (var reader = new InputStreamReader(resource, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        String value = properties.getProperty(id);
        if (value == null) throw new IOException("Нет ID " + id + " в " + fileName);
        if ("ERROR".equals(value)) throw new IOException("Имитируемый сбой источника " + fileName);
        return value;
    }

    public static void main(String[] args) {
        List<String> ids = args.length == 0
                ? List.of("org-1", "org-2", "org-3", "org-4", "org-5") : List.of(args);
        var collector = new AsyncDataCollector(
                id -> readProperty("organization-names.properties", id),
                id -> readProperty("organization-ratings.properties", id), raw -> {
                    Thread.sleep(50); // Имитация блокирующего обогащения названия.
                    return normalizeName(raw);
                }, DEFAULT_POOL_SIZE);
        Thread shutdownHook = new Thread(collector::close, "data-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try (collector) {
            long started = System.nanoTime();
            List<FinalResult> sync = collector.fetchAllDataSync(ids);
            double syncMillis = (System.nanoTime() - started) / 1_000_000.0;
            started = System.nanoTime();
            List<FinalResult> async = collector.fetchAllDataAsync(ids);
            double asyncMillis = (System.nanoTime() - started) / 1_000_000.0;
            if (!sync.equals(async)) throw new IllegalStateException("Результаты режимов отличаются");
            if (args.length == 0 && !async.stream().map(FinalResult::id).toList()
                    .equals(List.of("org-1", "org-2", "org-4", "org-5"))) {
                throw new IllegalStateException("Ожидался частичный результат без org-3");
            }
            System.out.println("Final results: " + async);
            System.out.printf(Locale.ROOT, "Successful: %d/%d; pool size: %d%n",
                    async.size(), ids.size(), DEFAULT_POOL_SIZE);
            System.out.printf(Locale.ROOT, "Sync: %.2f ms; async: %.2f ms; speedup: %.2fx%n",
                    syncMillis, asyncMillis, syncMillis / asyncMillis);
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException shutdownInProgress) {
                // При завершении JVM зарегистрированный hook уже закрывает сборщик.
            }
        }
    }
}
