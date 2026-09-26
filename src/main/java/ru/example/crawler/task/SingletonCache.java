package ru.example.crawler.task;

import java.util.concurrent.atomic.AtomicReference;

/** Один кэш на загрузчик классов, одно неизменяемое опубликованное значение. */
public final class SingletonCache {
    private static final SingletonCache INSTANCE = new SingletonCache();
    private final AtomicReference<String> value = new AtomicReference<>();

    private SingletonCache() {
    }

    public static SingletonCache getInstance() {
        return INSTANCE;
    }

    public String get() {
        String current = value.get();
        if (current != null) {
            return current;
        }
        // Создание кандидата не должно иметь побочных эффектов: кандидатов может быть несколько.
        String candidate = new String("cached value");
        return value.compareAndSet(null, candidate) ? candidate : value.get();
    }
}
