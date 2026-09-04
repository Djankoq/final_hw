package ru.example.crawler.task;

/** Демонстрация задачи, реализующей Runnable. */
public class LoggerTask implements Runnable {

    private final int iterations;

    public LoggerTask(int iterations) {
        this.iterations = iterations;
    }

    @Override
    public void run() {
        for (int i = 1; i <= iterations; i++) {
            System.out.printf("%s: %d%n", Thread.currentThread().getName(), i);
            pause();
        }
    }

    private void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
