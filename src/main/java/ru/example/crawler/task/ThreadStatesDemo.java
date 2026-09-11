package ru.example.crawler.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadStatesDemo {

    public static void main(String[] args) throws InterruptedException {
        new ThreadStatesDemo().run();
    }

    public void run() throws InterruptedException {
        Object monitor = new Object();
        AtomicBoolean leaveRunnable = new AtomicBoolean();
        CountDownLatch leaveWaiting = new CountDownLatch(1);
        CountDownLatch leaveTimedWaiting = new CountDownLatch(1);
        Thread[] workers = new Thread[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(() -> {
                try {
                    while (!leaveRunnable.get()) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        Thread.onSpinWait();
                    }
                    leaveWaiting.await();
                    synchronized (monitor) {
                    }
                    while (!leaveTimedWaiting.await(30, TimeUnit.SECONDS)) {
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }, "DemoWorker-" + (i + 1));
        }

        try {
            observe(workers, Thread.State.NEW);
            synchronized (monitor) {
                for (Thread worker : workers) {
                    worker.start();
                }
                observe(workers, Thread.State.RUNNABLE);
                leaveRunnable.set(true);
                observe(workers, Thread.State.WAITING);
                leaveWaiting.countDown();
                observe(workers, Thread.State.BLOCKED);
            }
            observe(workers, Thread.State.TIMED_WAITING);
            leaveTimedWaiting.countDown();
            for (Thread worker : workers) {
                worker.join();
            }
            observe(workers, Thread.State.TERMINATED);
            System.out.println("Все три потока завершены.");
        } finally {
            leaveRunnable.set(true);
            leaveWaiting.countDown();
            leaveTimedWaiting.countDown();
            for (Thread worker : workers) {
                worker.interrupt();
            }
            boolean interrupted = false;
            for (Thread worker : workers) {
                while (worker.isAlive()) {
                    try {
                        worker.join();
                    } catch (InterruptedException exception) {
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void observe(Thread[] workers, Thread.State expected) throws InterruptedException {
        for (Thread worker : workers) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Thread.State actual = worker.getState();
            while (actual != expected) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException(worker.getName() + ": ожидалось "
                            + expected + ", получено " + actual);
                }
                Thread.sleep(1);
                actual = worker.getState();
            }
            System.out.printf("%s -> %s%n", worker.getName(), actual);
        }
    }
}
