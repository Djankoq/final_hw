package ru.example.crawler.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RecursiveTask;

/** Рекурсивно считает агрегированные элементы, выделяя отдельную задачу на пачку. */
public final class AggregatedCountTask extends RecursiveTask<AggregatedCountTask.Result> {
    public record Result(long elementCount, int batchCount) { }

    private final List<?> elements;
    private final int from;
    private final int to;
    private final int batchSize;

    /** Создаёт снимок списка; null не считается агрегированным элементом. */
    public AggregatedCountTask(List<?> elements, int batchSize) {
        Objects.requireNonNull(elements, "Список элементов не должен быть null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Размер пачки должен быть положительным");
        }
        this.elements = new ArrayList<>(elements);
        this.from = 0;
        this.to = this.elements.size();
        this.batchSize = batchSize;
    }

    private AggregatedCountTask(List<?> elements, int from, int to, int batchSize) {
        this.elements = elements;
        this.from = from;
        this.to = to;
        this.batchSize = batchSize;
    }

    @Override
    protected Result compute() {
        if (from == to) {
            return new Result(0, 0);
        }
        if (to - from <= batchSize) {
            long count = 0;
            for (int i = from; i < to; i++) {
                if (elements.get(i) != null) {
                    count++;
                }
            }
            return new Result(count, 1);
        }

        int middle = from + (to - from) / 2;
        // Подзадачи используют один снимок, но обрабатывают непересекающиеся диапазоны.
        AggregatedCountTask left = new AggregatedCountTask(elements, from, middle, batchSize);
        AggregatedCountTask right = new AggregatedCountTask(elements, middle, to, batchSize);
        left.fork();
        Result rightResult = right.compute();
        Result leftResult = left.join();
        return new Result(leftResult.elementCount() + rightResult.elementCount(),
                leftResult.batchCount() + rightResult.batchCount());
    }
}
