package ru.example.crawler.task;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/** Асинхронная отправка GET-запросов через собственный пул рабочих потоков. */
public final class HttpRequestsDemo implements AutoCloseable {
    private final ThreadPoolExecutor executor;
    private final HttpClient client;
    private final Duration requestTimeout;

    public HttpRequestsDemo() {
        this(4, 8, 16, Duration.ofSeconds(10));
    }

    public HttpRequestsDemo(int coreThreads, int maxThreads, int queueCapacity, Duration requestTimeout) {
        if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("Таймаут должен быть положительным");
        }
        this.requestTimeout = requestTimeout;
        AtomicInteger threadNumber = new AtomicInteger();
        executor = new ThreadPoolExecutor(coreThreads, maxThreads, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                task -> new Thread(task, "http-worker-" + threadNumber.incrementAndGet()),
                new ThreadPoolExecutor.AbortPolicy());
        client = HttpClient.newBuilder()
                .connectTimeout(requestTimeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public record Result(String url, Integer statusCode, Duration elapsed, String error) {
        public boolean successful() {
            return statusCode != null && statusCode >= 200 && statusCode < 300;
        }

        public double elapsedMillis() {
            return elapsed.toNanos() / 1_000_000.0;
        }
    }

    public record Summary(List<Result> results, Duration elapsed) {
        public Summary {
            results = List.copyOf(results);
        }

        public void print(PrintStream out) {
            out.printf("%-7s %12s  %s%n", "HTTP", "Время, мс", "URL / ошибка");
            Map<Integer, Integer> statuses = new TreeMap<>();
            long successes = 0;
            long failures = 0;
            for (Result result : results) {
                out.printf(Locale.ROOT, "%-7s %12.2f  %s%s%n",
                        result.statusCode() == null ? "ERROR" : result.statusCode(),
                        result.elapsedMillis(), result.url(),
                        result.error() == null ? "" : " / " + result.error());
                if (result.successful()) successes++;
                if (result.statusCode() == null) failures++;
                else statuses.merge(result.statusCode(), 1, Integer::sum);
            }
            var timings = results.stream().filter(result -> result.statusCode() != null)
                    .mapToDouble(Result::elapsedMillis).summaryStatistics();
            out.printf("Всего: %d; успешных (2xx): %d; HTTP вне 2xx: %d; ошибок: %d%n",
                    results.size(), successes, results.size() - successes - failures, failures);
            out.println("Статус-коды: " + statuses);
            out.printf(Locale.ROOT, "Общее время: %.2f мс%n", elapsed.toNanos() / 1_000_000.0);
            if (timings.getCount() > 0) {
                out.printf(Locale.ROOT, "Время HTTP-ответов: min=%.2f; avg=%.2f; max=%.2f мс%n",
                        timings.getMin(), timings.getAverage(), timings.getMax());
            }
        }
    }

    /** Возвращается сразу; ожидание всех результатов выполняет вызывающий код. */
    public CompletableFuture<Summary> fetchAll(List<String> urls) {
        List<String> inputs = List.copyOf(urls);
        long started = System.nanoTime();
        List<CompletableFuture<Result>> requests = new ArrayList<>();
        for (String url : inputs) {
            RequestTask task = new RequestTask(url);
            requests.add(task.future);
            try {
                // Блокирующий HTTP-вызов выполняется только внутри нашего пула.
                // Пул HttpClient не используется для запуска пользовательских задач.
                executor.execute(task);
            } catch (RejectedExecutionException rejected) {
                task.fail("Пул закрыт или очередь запросов переполнена");
            }
        }
        return CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> new Summary(requests.stream().map(CompletableFuture::join).toList(),
                        Duration.ofNanos(System.nanoTime() - started)));
    }

    private final class RequestTask implements Runnable {
        private final String url;
        private final CompletableFuture<Result> future = new CompletableFuture<>();

        private RequestTask(String url) {
            this.url = url;
        }

        @Override
        public void run() {
            try {
                future.complete(fetch(url));
            } catch (Throwable failure) {
                future.completeExceptionally(failure);
                throw failure;
            }
        }

        private void fail(String error) {
            future.complete(new Result(url, null, Duration.ZERO, error));
        }
    }

    private Result fetch(String url) {
        long started = System.nanoTime();
        try {
            URI uri = URI.create(url);
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null) {
                throw new IllegalArgumentException("Нужен абсолютный URL с протоколом HTTP или HTTPS");
            }
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(requestTimeout).GET().build();
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            return new Result(url, response.statusCode(), elapsedSince(started), null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new Result(url, null, elapsedSince(started), "Запрос прерван");
        } catch (IOException | IllegalArgumentException failure) {
            return new Result(url, null, elapsedSince(started),
                    failure.getClass().getSimpleName() + ": " + failure.getMessage());
        }
    }

    private static Duration elapsedSince(long started) {
        return Duration.ofNanos(System.nanoTime() - started);
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                finishQueued(executor.shutdownNow());
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            finishQueued(executor.shutdownNow());
            Thread.currentThread().interrupt();
        }
    }

    private void finishQueued(List<Runnable> queued) {
        queued.forEach(task -> ((RequestTask) task).fail("Запрос отменён при закрытии пула"));
    }

    public static void main(String[] args) throws Exception {
        List<String> urls;
        if (args.length == 0) {
            urls = IntStream.rangeClosed(1, 20)
                    .mapToObj(id -> "https://jsonplaceholder.typicode.com/todos/" + id).toList();
        } else if (args.length == 2 && "--file".equals(args[0])) {
            urls = Files.readAllLines(Path.of(args[1])).stream().map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#")).toList();
        } else if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--"))) {
            throw new IllegalArgumentException("Использование: [URL ...] или --file urls.txt");
        } else {
            urls = List.of(args);
        }
        try (HttpRequestsDemo demo = new HttpRequestsDemo()) {
            demo.fetchAll(urls).get().print(System.out);
        }
    }
}
