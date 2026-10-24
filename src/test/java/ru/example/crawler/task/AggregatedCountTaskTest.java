package ru.example.crawler.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class AggregatedCountTaskTest {
    @Test
    void countsElementsAcrossBatchBoundaries() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(2);
        try {
            for (int size : new int[]{0, 1, 2, 3, 4, 5, 17, 10_001}) {
                List<Integer> elements = IntStream.range(0, size).boxed().toList();
                for (int batchSize : new int[]{1, 2, 4, 256, 10_001}) {
                    var result = pool.invoke(new AggregatedCountTask(elements, batchSize));
                    assertEquals(size, result.elementCount(), "size=" + size + ", batchSize=" + batchSize);
                    if (size == 0) {
                        assertEquals(0, result.batchCount());
                    } else if (size <= batchSize) {
                        assertEquals(1, result.batchCount());
                    } else {
                        assertTrue(result.batchCount() >= (size + batchSize - 1) / batchSize);
                        assertTrue(result.batchCount() <= size);
                    }
                }
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void recursivelySplitsUnevenRangesIntoSeparateBatches() throws Exception {
        var result = count(IntStream.range(0, 17).boxed().toList(), 4);
        assertEquals(17, result.elementCount());
        // 17 -> 8 + 9 -> (4 + 4) + (4 + (2 + 3)): пять листовых задач.
        assertEquals(5, result.batchCount());
    }

    @Test
    void keepsDuplicatesSkipsNullsAndUsesSnapshot() throws Exception {
        var elements = new ArrayList<>(Arrays.asList("A", null, "A", "B", "C"));
        var task = new AggregatedCountTask(elements, 2);
        elements.clear();
        ForkJoinPool pool = new ForkJoinPool(2);
        try {
            var result = pool.invoke(task);
            assertEquals(4, result.elementCount());
            assertEquals(3, result.batchCount());
            assertTrue(elements.isEmpty());
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectsInvalidInput() {
        assertThrows(NullPointerException.class, () -> new AggregatedCountTask(null, 2));
        assertThrows(IllegalArgumentException.class, () -> new AggregatedCountTask(List.of(), 0));
        assertThrows(IllegalArgumentException.class, () -> new AggregatedCountTask(List.of("A"), -1));
    }

    private static AggregatedCountTask.Result count(List<?> elements, int batchSize) throws Exception {
        ForkJoinPool pool = new ForkJoinPool(2);
        try {
            return pool.invoke(new AggregatedCountTask(elements, batchSize));
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}
