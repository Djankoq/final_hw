package ru.example.crawler.task;

import ru.example.crawler.service.AsyncDataCollector;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Считает результаты сборщика из предыдущего задания через отдельный ForkJoinPool. */
public final class ForkJoinCountDemo {
    private ForkJoinCountDemo() { }

    public static void main(String[] args) {
        int batchSize = args.length == 0 ? 2 : Integer.parseInt(args[0]);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Размер пачки должен быть положительным");
        }
        List<String> ids = args.length <= 1
                ? List.of("org-1", "org-2", "org-3", "org-4", "org-5")
                : Arrays.asList(args).subList(1, args.length);
        int parallelism = Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors()));
        AtomicInteger threadNumber = new AtomicInteger();
        ForkJoinPool pool = new ForkJoinPool(parallelism, owner -> {
            var worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(owner);
            worker.setName("aggregate-counter-" + threadNumber.incrementAndGet());
            return worker;
        }, null, false);
        try (var collector = AsyncDataCollector.createFileCollector()) {
            // Получение данных завершено до запуска ForkJoin: внутри compute() нет блокирующего I/O.
            List<AsyncDataCollector.FinalResult> results = collector.fetchAllDataAsync(ids);
            AggregatedCountTask.Result count = pool.invoke(new AggregatedCountTask(results, batchSize));
            if (count.elementCount() != results.size()) {
                throw new IllegalStateException("Подсчёт ForkJoin не совпал с количеством результатов");
            }
            System.out.printf("Requested IDs: %d; aggregated elements: %d%n", ids.size(), count.elementCount());
            System.out.printf("Batch size: %d; processed batches: %d; parallelism: %d%n",
                    batchSize, count.batchCount(), parallelism);
        } finally {
            // В Java 17 ForkJoinPool ещё не поддерживает try-with-resources.
            pool.shutdown();
            try {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                    if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                        System.err.println("Пул ForkJoin не завершился после отмены задач");
                    }
                }
            } catch (InterruptedException interrupted) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
