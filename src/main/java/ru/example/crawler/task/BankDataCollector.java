package ru.example.crawler.task;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Все суммы хранятся в копейках. Один монитор защищает весь банк и журнал. */
public final class BankDataCollector {
    public record Item(String key, String from, String to, long amount) {
        public Item {
            Objects.requireNonNull(key);
            Objects.requireNonNull(from);
            Objects.requireNonNull(to);
            if (key.isBlank() || from.equals(to) || amount <= 0) {
                throw new IllegalArgumentException("Invalid transfer");
            }
        }
    }

    private final Map<String, Long> balances = new LinkedHashMap<>();
    private final Map<String, Item> processed = new LinkedHashMap<>();
    private long processedCount;

    public synchronized void openAccount(String id, long balance) {
        Objects.requireNonNull(id);
        if (id.isBlank() || balance < 0 || balances.containsKey(id)) {
            throw new IllegalArgumentException("Invalid or existing account: " + id);
        }
        balances.put(id, balance);
    }

    public synchronized long getBalance(String id) {
        Long balance = balances.get(id);
        if (balance == null) {
            throw new IllegalArgumentException("Unknown account: " + id);
        }
        return balance;
    }

    public synchronized void deposit(String id, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Amount must be positive");
        }
        balances.put(id, Math.addExact(getBalance(id), amount));
        notifyAll();
    }

    /** Возвращает false для дубликата или при недостатке денег, без ожидания. */
    public synchronized boolean collectItem(Item item) {
        Objects.requireNonNull(item);
        Item previous = processed.get(item.key());
        if (previous != null) {
            if (!previous.equals(item)) {
                throw new IllegalArgumentException("Transfer key reused with different data");
            }
            return false;
        }
        long source = getBalance(item.from());
        long target = getBalance(item.to());
        if (source < item.amount()) {
            return false;
        }
        // Проверяем переполнение до первого изменения состояния.
        long updatedTarget = Math.addExact(target, item.amount());
        balances.put(item.from(), source - item.amount());
        balances.put(item.to(), updatedTarget);
        processed.put(item.key(), item);
        incrementProcessed();
        notifyAll();
        return true;
    }

    /** Ожидание освобождает единственный монитор; второй ресурс не захватывается. */
    public synchronized boolean collectItem(Item item, long timeoutMillis) throws InterruptedException {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("Negative timeout");
        }
        long timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long start = System.nanoTime();
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            if (collectItem(item)) {
                return true;
            }
            if (isAlreadyProcessed(item.key())) {
                return false;
            }
            long remaining = timeout - (System.nanoTime() - start);
            if (remaining <= 0) {
                return false;
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }

    private synchronized void incrementProcessed() {
        processedCount++;
    }

    public synchronized boolean isAlreadyProcessed(String key) {
        return processed.containsKey(Objects.requireNonNull(key));
    }

    public synchronized long getProcessedCount() {
        return processedCount;
    }

    public synchronized List<Item> getItems() {
        return List.copyOf(processed.values());
    }

    public synchronized Map<String, Long> getBalances() {
        return Map.copyOf(balances);
    }
}
