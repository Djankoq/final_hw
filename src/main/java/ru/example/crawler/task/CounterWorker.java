package ru.example.crawler.task;

/** Демонстрация создания задачи наследованием от Thread. */
public class CounterWorker extends Thread {

    private final int iterations;

    public CounterWorker(int iterations) {
        super("CounterWorker");
        this.iterations = iterations;
    }

    @Override
    public void run() {
        for (int i = 1; i <= iterations; i++) {
            System.out.printf("%s: %d%n", getName(), i);
            pause();
        }
    }

    private void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException exception) {
            interrupt();
        }
    }
}
