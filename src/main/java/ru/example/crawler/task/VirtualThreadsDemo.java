package ru.example.crawler.task;

import com.sun.management.OperatingSystemMXBean;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Сравнение отдельного потока на задачу: Java 21, --enable-preview. */
public final class VirtualThreadsDemo {
    public enum Mode { VIRTUAL, PLATFORM, STRUCTURED }

    @FunctionalInterface
    public interface Task {
        void run(int taskId) throws Exception;
    }

    public record Measurement(Mode mode, int completed, long elapsedNanos, long processCpuNanos,
                              long heapBeforeBytes, long sampledPeakHeapBytes,
                              int platformThreadsBefore, int sampledPeakPlatformThreads) {
        public double elapsedMillis() {
            return elapsedNanos / 1_000_000.0;
        }
    }

    /** null вместо out отключает только вывод имён для замеров без консольного I/O. */
    public static Task simulatedIoTask(long sleepMillis, PrintStream out) {
        if (sleepMillis < 0) {
            throw new IllegalArgumentException("sleepMillis must be non-negative");
        }
        return taskId -> {
            if (out != null) {
                out.println(Thread.currentThread().getName());
            }
            // Виртуальный поток паркуется, освобождая carrier; платформенный занимает OS thread.
            Thread.sleep(sleepMillis);
        };
    }

    /** Время включает создание, запуск и полное завершение всех потоков. */
    public Measurement measure(Mode mode, int taskCount, Task task)
            throws InterruptedException, ExecutionException {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(task, "task");
        if (taskCount <= 0) {
            throw new IllegalArgumentException("taskCount must be positive");
        }
        AtomicInteger completed = new AtomicInteger();
        Task counted = id -> {
            task.run(id);
            completed.incrementAndGet();
        };
        var resources = new ResourceMonitor();
        long elapsed;
        long cpuUsed;
        try (resources) {
            long cpuBefore = processCpuTime();
            long started = System.nanoTime();
            switch (mode) {
                case VIRTUAL, PLATFORM -> runThreads(mode, taskCount, counted);
                case STRUCTURED -> runStructured(taskCount, counted);
            }
            elapsed = System.nanoTime() - started;
            long cpuAfter = processCpuTime();
            cpuUsed = cpuBefore < 0 || cpuAfter < 0 ? -1 : cpuAfter - cpuBefore;
        }
        if (completed.get() != taskCount) {
            throw new IllegalStateException("Incomplete run: " + completed.get() + "/" + taskCount);
        }
        return new Measurement(mode, completed.get(), elapsed, cpuUsed,
                resources.heapBefore, resources.peakHeap,
                resources.threadsBefore, resources.peakThreads);
    }

    private void runThreads(Mode mode, int count, Task task)
            throws InterruptedException, ExecutionException {
        List<Thread> threads = new ArrayList<>(count);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            for (int id = 0; id < count && failure.get() == null; id++) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                int taskId = id;
                Runnable action = () -> {
                    try {
                        task.run(taskId);
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    }
                };
                if (mode == Mode.VIRTUAL) {
                    threads.add(Thread.ofVirtual().name("virtual-" + id).start(action));
                } else {
                    Thread thread = new Thread(action, "platform-" + id);
                    threads.add(thread);
                    thread.start();
                }
            }
            for (Thread thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new ExecutionException(failure.get());
            }
        } finally {
            // Даже при прерывании владельца или ошибке создания потока не оставляем фоновых задач.
            threads.forEach(Thread::interrupt);
            joinUninterruptibly(threads);
        }
    }

    private void runStructured(int count, Task task) throws InterruptedException, ExecutionException {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure(
                "io-comparison", Thread.ofVirtual().name("structured-", 0).factory())) {
            for (int id = 0; id < count; id++) {
                if (scope.isShutdown() || Thread.currentThread().isInterrupted()) {
                    break;
                }
                int taskId = id;
                scope.fork(() -> {
                    task.run(taskId);
                    return null;
                });
            }
            scope.join();
            scope.throwIfFailed();
        } // close() отменяет незавершённые задачи и ждёт их выхода.
    }

    private static void joinUninterruptibly(List<Thread> threads) {
        boolean interrupted = false;
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException error) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static long processCpuTime() {
        return ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean os
                ? os.getProcessCpuTime() : -1;
    }

    /** Приблизительные пики с периодом 5 мс. ThreadMXBean не учитывает виртуальные потоки. */
    private static final class ResourceMonitor implements AutoCloseable {
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        private final long heapBefore = memory.getHeapMemoryUsage().getUsed();
        private final int threadsBefore = threads.getThreadCount();
        private long peakHeap = heapBefore;
        private int peakThreads = threadsBefore;
        private volatile boolean running = true;
        private final Thread sampler;

        private ResourceMonitor() {
            sampler = new Thread(() -> {
                while (running) {
                    sample();
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException stopped) {
                        return;
                    }
                }
            }, "thread-resource-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }

        private void sample() {
            peakHeap = Math.max(peakHeap, memory.getHeapMemoryUsage().getUsed());
            peakThreads = Math.max(peakThreads, threads.getThreadCount());
        }

        @Override
        public void close() {
            if (running) {
                running = false;
                sampler.interrupt();
                joinUninterruptibly(List.of(sampler));
                sample();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int[] sizes = {10_000};
        long sleepMillis = 100;
        int rounds = 3;
        boolean quiet = false;
        List<Mode> modes = List.of(Mode.values());
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            if ("--quiet".equals(option)) {
                quiet = true;
            } else if ("--help".equals(option)) {
                System.out.println("--tasks 100,1000,10000 --sleep-ms 100 --rounds 3 --quiet"
                        + " --mode all|virtual|platform|structured");
                return;
            } else {
                if (i + 1 == args.length) {
                    throw new IllegalArgumentException("Missing value for " + option);
                }
                String value = args[++i];
                switch (option) {
                    case "--tasks" -> sizes = Arrays.stream(value.split(",", -1))
                            .mapToInt(Integer::parseInt).toArray();
                    case "--sleep-ms" -> sleepMillis = Long.parseLong(value);
                    case "--rounds" -> rounds = Integer.parseInt(value);
                    case "--mode" -> modes = "all".equals(value) ? List.of(Mode.values())
                            : List.of(Mode.valueOf(value.toUpperCase(Locale.ROOT)));
                    default -> throw new IllegalArgumentException("Unknown option: " + option);
                }
            }
        }
        if (rounds <= 0 || sleepMillis < 0 || Arrays.stream(sizes).anyMatch(size -> size <= 0)) {
            throw new IllegalArgumentException("Tasks/rounds must be positive; sleep-ms non-negative");
        }
        System.out.printf("Java %s; OS %s %s; processors=%d; sleep=%d ms; rounds=%d; quiet=%s%n",
                System.getProperty("java.version"), System.getProperty("os.name"),
                System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(),
                sleepMillis, rounds, quiet);
        System.out.println("CPU is process time; heap/platform-thread peaks are sampled every 5 ms."
                + " Heap excludes native thread stacks; the sampler adds one platform thread.");
        if (!quiet) {
            System.out.println("Thread-name printing is included in timings; use --quiet for comparison.");
        }
        VirtualThreadsDemo demo = new VirtualThreadsDemo();
        for (Mode mode : modes) {
            demo.measure(mode, 100, simulatedIoTask(sleepMillis, null));
        }
        System.out.println("Warmup: 100 tasks per mode (excluded). Heap values are absolute, not deltas.");
        System.out.println("tasks round mode completed wall_ms cpu_ms cpu_cores heap_before_MiB"
                + " heap_peak_MiB platform_before platform_peak");
        Task task = simulatedIoTask(sleepMillis, quiet ? null : System.out);
        for (int size : sizes) {
            double[][] times = new double[modes.size()][rounds];
            for (int round = 0; round < rounds; round++) {
                // Ротация порядка уменьшает систематическое преимущество первого режима.
                for (int offset = 0; offset < modes.size(); offset++) {
                    int index = (round + offset) % modes.size();
                    Measurement result = demo.measure(modes.get(index), size, task);
                    times[index][round] = result.elapsedMillis();
                    System.out.printf(Locale.ROOT,
                            "%d %d %s %d %.3f %.3f %.3f %.2f %.2f %d %d%n",
                            size, round + 1, result.mode(), result.completed(), result.elapsedMillis(),
                            result.processCpuNanos() < 0 ? -1 : result.processCpuNanos() / 1_000_000.0,
                            result.processCpuNanos() < 0 ? -1
                                    : (double) result.processCpuNanos() / result.elapsedNanos(),
                            result.heapBeforeBytes() / 1048576.0,
                            result.sampledPeakHeapBytes() / 1048576.0,
                            result.platformThreadsBefore(), result.sampledPeakPlatformThreads());
                }
            }
            for (int i = 0; i < modes.size(); i++) {
                Arrays.sort(times[i]);
                double median = (times[i][(rounds - 1) / 2] + times[i][rounds / 2]) / 2;
                System.out.printf(Locale.ROOT, "Median: tasks=%d mode=%s wall_ms=%.3f%n",
                        size, modes.get(i), median);
            }
        }
    }
}
