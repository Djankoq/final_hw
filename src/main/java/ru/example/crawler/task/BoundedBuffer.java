package ru.example.crawler.task;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Потокобезопасный FIFO-буфер фиксированной вместимости. Null запрещён. */
public final class BoundedBuffer<T> {
    private final ArrayDeque<T> items = new ArrayDeque<>();
    private final int capacity;
    private final boolean wakeAll;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public BoundedBuffer(int capacity) {
        this(capacity, false);
    }

    /** wakeAll включает signalAll вместо signal для сравнения уведомлений. */
    public BoundedBuffer(int capacity, boolean wakeAll) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.wakeAll = wakeAll;
    }

    /** Ожидает свободного места; прерывание await передаётся вызывающему коду. */
    public void put(T item) throws InterruptedException {
        Objects.requireNonNull(item);
        lock.lock();
        try {
            while (items.size() == capacity) {
                notFull.await();
            }
            items.addLast(item);
            notifyWaiters(notEmpty);
        } finally {
            lock.unlock();
        }
    }

    /** Ожидает элемент. После пробуждения условие обязательно проверяется снова. */
    public T take() throws InterruptedException {
        lock.lock();
        try {
            while (items.isEmpty()) {
                notEmpty.await();
            }
            T item = items.removeFirst();
            notifyWaiters(notFull);
            return item;
        } finally {
            lock.unlock();
        }
    }

    private void notifyWaiters(Condition condition) {
        if (wakeAll) {
            condition.signalAll();
        } else {
            condition.signal();
        }
    }
}
