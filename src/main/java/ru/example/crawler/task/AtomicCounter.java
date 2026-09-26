package ru.example.crawler.task;

import java.util.concurrent.atomic.AtomicInteger;

public final class AtomicCounter {
    private final AtomicInteger value = new AtomicInteger();

    public int increment() {
        return value.incrementAndGet();
    }

    public int get() {
        return value.get();
    }
}
