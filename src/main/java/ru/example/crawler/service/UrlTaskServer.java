package ru.example.crawler.service;

import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Производители передают URL потребителям через ограниченную BlockingQueue. */
public final class UrlTaskServer {
    private static final Task STOP = new Task(-1, "");

    private final int producerCount;
    private final int consumerCount;
    private final int queueCapacity;
    private final Duration requestTimeout;
    private final HttpClient client;

    public UrlTaskServer() {
        this(2, 3, 8, Duration.ofSeconds(10));
    }

    public UrlTaskServer(int producerCount, int consumerCount, int queueCapacity, Duration requestTimeout) {
        if (producerCount <= 0 || consumerCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("Количество производителей, потребителей и ёмкость должны быть положительными");
        }
        if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("Таймаут должен быть положительным");
        }
        this.producerCount = producerCount;
        this.consumerCount = consumerCount;
        this.queueCapacity = queueCapacity;
        this.requestTimeout = requestTimeout;
        client = HttpClient.newBuilder()
                .connectTimeout(requestTimeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public record Result(String url, Integer statusCode, String title, String error, String consumer) {
        public boolean successful() {
            return error == null;
        }
    }

    private record Task(int index, String url) { }

    /** Возвращает результаты в порядке входного списка, сохраняя повторяющиеся URL. */
    public List<Result> process(List<String> urls) throws InterruptedException {
        List<String> inputs = List.copyOf(urls);
        if (inputs.isEmpty()) return List.of();

        BlockingQueue<Task> queue = new ArrayBlockingQueue<>(queueCapacity);
        Result[] results = new Result[inputs.size()];
        ExecutorService producers = newPool(producerCount, "url-producer-");
        ExecutorService consumers = newPool(consumerCount, "url-consumer-");
        try {
            List<Future<Void>> consumption = new ArrayList<>();
            for (int i = 0; i < consumerCount; i++) {
                consumption.add(consumers.submit(new Consumer(queue, results)));
            }
            List<Future<Void>> production = new ArrayList<>();
            for (int i = 0; i < producerCount; i++) {
                production.add(producers.submit(new Producer(i, inputs, queue)));
            }
            awaitAll(production);

            // Только координатор посылает сигналы остановки: все URL уже в очереди.
            // Каждый потребитель забирает один сигнал и завершает свою работу.
            for (int i = 0; i < consumerCount; i++) queue.put(STOP);
            awaitAll(consumption);
            return List.of(results);
        } finally {
            // Прерывание снимает ожидание put(), take() и HTTP-запроса.
            producers.shutdownNow();
            consumers.shutdownNow();
            awaitTermination(producers, consumers);
        }
    }

    private final class Producer implements Callable<Void> {
        private final int number;
        private final List<String> urls;
        private final BlockingQueue<Task> queue;

        private Producer(int number, List<String> urls, BlockingQueue<Task> queue) {
            this.number = number;
            this.urls = urls;
            this.queue = queue;
        }

        @Override
        public Void call() throws InterruptedException {
            for (int i = number; i < urls.size(); i += producerCount) {
                // Заполненная очередь блокирует производителя, задачи не теряются.
                queue.put(new Task(i, urls.get(i)));
            }
            return null;
        }
    }

    private final class Consumer implements Callable<Void> {
        private final BlockingQueue<Task> queue;
        private final Result[] results;

        private Consumer(BlockingQueue<Task> queue, Result[] results) {
            this.queue = queue;
            this.results = results;
        }

        @Override
        public Void call() throws InterruptedException {
            while (true) {
                Task task = queue.take();
                if (task == STOP) return null;
                // У каждой задачи свой индекс; get() всех Future обеспечивает видимость результатов.
                results[task.index()] = fetch(task.url());
            }
        }
    }

    private Result fetch(String url) throws InterruptedException {
        String consumer = Thread.currentThread().getName();
        Integer status = null;
        try {
            URI uri = URI.create(url);
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null) {
                throw new IllegalArgumentException("Нужен абсолютный URL с протоколом HTTP или HTTPS");
            }
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(requestTimeout).GET().build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            status = response.statusCode();
            if (status < 200 || status >= 300) {
                return new Result(url, status, null, "HTTP " + status, consumer);
            }
            String title = extractTitle(response.body());
            if (title.isEmpty()) {
                return new Result(url, status, null, "Заголовок <title> не найден или пуст", consumer);
            }
            return new Result(url, status, title, null, consumer);
        } catch (IOException | RuntimeException failure) {
            // Ошибка одного URL не останавливает потребителя и остальную очередь.
            return new Result(url, status, null,
                    failure.getClass().getSimpleName() + ": " + failure.getMessage(), consumer);
        }
    }

    private static String extractTitle(String html) throws IOException {
        StringBuilder title = new StringBuilder();
        new ParserDelegator().parse(new StringReader(html), new HTMLEditorKit.ParserCallback() {
            private boolean inTitle;
            private boolean finished;

            @Override
            public void handleStartTag(HTML.Tag tag, javax.swing.text.MutableAttributeSet attributes, int position) {
                if (tag == HTML.Tag.TITLE && !finished) inTitle = true;
            }

            @Override
            public void handleText(char[] text, int position) {
                if (inTitle) title.append(text);
            }

            @Override
            public void handleEndTag(HTML.Tag tag, int position) {
                if (tag == HTML.Tag.TITLE && inTitle) {
                    inTitle = false;
                    finished = true;
                }
            }
        }, true);
        return title.toString().replaceAll("(?U)\\s+", " ").strip();
    }

    private static ExecutorService newPool(int count, String prefix) {
        AtomicInteger number = new AtomicInteger();
        return Executors.newFixedThreadPool(count,
                task -> new Thread(task, prefix + number.incrementAndGet()));
    }

    private static void awaitAll(List<Future<Void>> futures) throws InterruptedException {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException failure) {
                throw new IllegalStateException("Сбой потока сервера задач", failure.getCause());
            }
        }
    }

    private static void awaitTermination(ExecutorService... pools) {
        boolean interrupted = false;
        try {
            for (ExecutorService pool : pools) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!pool.isTerminated()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) throw new IllegalStateException("Потоки сервера задач не завершились");
                    try {
                        pool.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException interruption) {
                        interrupted = true;
                    }
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
