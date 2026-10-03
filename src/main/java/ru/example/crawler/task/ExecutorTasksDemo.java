package ru.example.crawler.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Future, invokeAll и периодическая фоновая операция. */
public final class ExecutorTasksDemo {
    private ExecutorTasksDemo() { }

    public static void main(String[] args) throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(5);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var background = scheduler.scheduleAtFixedRate(
                    () -> System.out.println("Фоновая проверка: система работает"),
                    0, 2, TimeUnit.SECONDS);

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(1);
            Future<Integer> longTask = workers.submit(() -> {
                try {
                    System.out.println("Долгая задача: начало обработки (8 секунд)");
                    started.countDown();
                    TimeUnit.SECONDS.sleep(8);
                    return 42;
                } catch (InterruptedException interrupted) {
                    System.out.println("Долгая задача: обработка прервана");
                    Thread.currentThread().interrupt();
                    throw interrupted;
                } finally {
                    finished.countDown();
                }
            });
            if (!started.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Долгая задача не запустилась");
            }
            TimeUnit.MILLISECONDS.sleep(2500);
            System.out.println("cancel(true): " + longTask.cancel(true));
            System.out.printf("isCancelled=%s, isDone=%s%n", longTask.isCancelled(), longTask.isDone());
            try {
                System.out.println("Результат: " + longTask.get());
            } catch (CancellationException cancelled) {
                System.out.println("get(): CancellationException — задача отменена");
            }
            // isDone() не гарантирует фактический выход рабочего потока.
            if (!finished.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Задача не завершилась после прерывания");
            }

            List<Callable<Integer>> tasks = new ArrayList<>();
            for (int id = 1; id <= 5; id++) {
                int taskId = id;
                tasks.add(() -> {
                    int seconds = ThreadLocalRandom.current().nextInt(1, 4);
                    System.out.printf("Задача %d: работа %d с, поток %s%n",
                            taskId, seconds, Thread.currentThread().getName());
                    TimeUnit.SECONDS.sleep(seconds);
                    return taskId * 10;
                });
            }
            List<Future<Integer>> results = workers.invokeAll(tasks);
            for (int i = 0; i < results.size(); i++) {
                System.out.printf("Результат задачи %d: %d%n", i + 1, results.get(i).get());
            }
            background.cancel(false);
        } finally {
            shutdown(scheduler);
            shutdown(workers);
        }
    }

    private static void shutdown(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
