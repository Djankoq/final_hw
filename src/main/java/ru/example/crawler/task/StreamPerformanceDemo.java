package ru.example.crawler.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

public class StreamPerformanceDemo {
    private static final int SIZE = 1_000_000;
    private static final int WARMUP = 10;
    private static final int ROUNDS = 15;

    public static void main(String[] args) {
        new StreamPerformanceDemo().run();
    }

    public void run() {
        Random random = new Random(42);
        List<Integer> numbers = new ArrayList<>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            numbers.add(random.nextInt(1_000_000));
        }

        long expectedSum = 0;
        int evenCount = 0;
        for (int number : numbers) {
            if (number % 2 == 0) {
                evenCount++;
                expectedSum += number * 2L;
            }
        }
        System.out.printf("Java: %s; processors: %d; common pool parallelism: %d%n",
                System.getProperty("java.version"), Runtime.getRuntime().availableProcessors(),
                ForkJoinPool.getCommonPoolParallelism());
        System.out.printf("Size: %d; seed: 42; range: [0, 1000000)%n", SIZE);
        System.out.printf("Even count: %d; expected sum: %d%n", evenCount, expectedSum);
        System.out.printf("Warmup: %d per variant; measured rounds: %d%n", WARMUP, ROUNDS);

        for (int i = 0; i < WARMUP; i++) {
            measure(numbers, i % 2 == 0, expectedSum);
            measure(numbers, i % 2 != 0, expectedSum);
        }
        double[] sequential = new double[ROUNDS];
        double[] parallel = new double[ROUNDS];
        for (int i = 0; i < ROUNDS; i++) {
            if (i % 2 == 0) {
                sequential[i] = measure(numbers, false, expectedSum);
                parallel[i] = measure(numbers, true, expectedSum);
            } else {
                parallel[i] = measure(numbers, true, expectedSum);
                sequential[i] = measure(numbers, false, expectedSum);
            }
        }
        for (int i = 0; i < ROUNDS; i++) {
            System.out.printf(Locale.ROOT, "Round %02d: stream=%.3f ms; parallelStream=%.3f ms%n",
                    i + 1, sequential[i], parallel[i]);
        }
        printStats("stream", sequential);
        printStats("parallelStream", parallel);
        System.out.printf(Locale.ROOT, "Median speedup (stream / parallelStream): %.3fx%n",
                median(sequential) / median(parallel));
        System.out.printf("Both sums: %d; all results verified against a loop.%n", expectedSum);
    }

    private double measure(List<Integer> numbers, boolean parallel, long expectedSum) {
        long start = System.nanoTime();
        Stream<Integer> stream = parallel ? numbers.parallelStream() : numbers.stream();
        long sum = stream.filter(number -> number % 2 == 0)
                .mapToLong(number -> number * 2L)
                .sum();
        long elapsed = System.nanoTime() - start;
        if (sum != expectedSum) {
            throw new IllegalStateException("Incorrect sum: " + sum + "; expected: " + expectedSum);
        }
        return elapsed / 1_000_000.0;
    }

    private void printStats(String name, double[] times) {
        System.out.printf(Locale.ROOT, "%s: min=%.3f ms; median=%.3f ms; mean=%.3f ms; max=%.3f ms%n",
                name, Arrays.stream(times).min().orElseThrow(), median(times),
                Arrays.stream(times).average().orElseThrow(), Arrays.stream(times).max().orElseThrow());
    }

    private double median(double[] times) {
        double[] sorted = times.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }
}
