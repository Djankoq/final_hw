package ru.example.crawler.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class BankDataCollectorTest {
    private BankDataCollector bank(long balance) {
        var bank = new BankDataCollector();
        bank.openAccount("A", balance);
        bank.openAccount("B", 0);
        return bank;
    }

    @Test
    void concurrentDuplicatesAndOppositeTransfers() throws Exception {
        var bank = bank(100_000);
        bank.deposit("B", 100_000);
        BankPerformanceDemo.runConcurrent(8, worker -> {
            for (int i = 0; i < 2_000; i++) {
                String from = i % 2 == 0 ? "A" : "B";
                String to = i % 2 == 0 ? "B" : "A";
                bank.collectItem(new BankDataCollector.Item("id-" + i, from, to, 1));
                var snapshot = bank.getBalances();
                assertEquals(200_000L, snapshot.get("A") + snapshot.get("B"));
            }
        });
        assertEquals(2_000, bank.getProcessedCount());
        assertEquals(2_000, bank.getItems().size());
        assertEquals(100_000, bank.getBalance("A"));
        assertEquals(100_000, bank.getBalance("B"));
        assertTrue(bank.isAlreadyProcessed("id-0"));
        assertThrows(UnsupportedOperationException.class, () -> bank.getItems().clear());
    }

    @Test
    void depositWakesAllWaitersAndDuplicateIsNotRepeated() throws Exception {
        var bank = bank(0);
        var item = new BankDataCollector.Item("same", "A", "B", 10);
        var first = new FutureTask<>(() -> bank.collectItem(item, 5_000));
        var second = new FutureTask<>(() -> bank.collectItem(item, 5_000));
        Thread one = new Thread(first);
        Thread two = new Thread(second);
        one.start();
        two.start();
        try {
            awaitWaiting(one);
            awaitWaiting(two);
            bank.deposit("A", 10);
            assertNotEquals(first.get(2, TimeUnit.SECONDS), second.get(2, TimeUnit.SECONDS));
            assertEquals(1, bank.getProcessedCount());
            assertEquals(10, bank.getBalance("B"));
        } finally {
            one.interrupt();
            two.interrupt();
            one.join(2_000);
            two.join(2_000);
        }
    }

    @Test
    void timeoutAndInterruptionDoNotChangeBalances() throws Exception {
        var bank = bank(0);
        var item = new BankDataCollector.Item("waiting", "A", "B", 10);
        assertFalse(bank.collectItem(item, 20));
        var task = new FutureTask<>(() -> {
            assertThrows(InterruptedException.class, () -> bank.collectItem(item, 5_000));
            return null;
        });
        Thread thread = new Thread(task);
        thread.start();
        try {
            awaitWaiting(thread);
            thread.interrupt();
            task.get(2, TimeUnit.SECONDS);
            assertEquals(0, bank.getProcessedCount());
            assertEquals(0, bank.getBalance("A"));
            assertEquals(0, bank.getBalance("B"));
        } finally {
            thread.interrupt();
            thread.join(2_000);
        }
    }

    @Test
    void invalidTransfersAndOverflowLeaveStateIntact() {
        var bank = bank(10);
        bank.deposit("B", Long.MAX_VALUE);
        assertThrows(ArithmeticException.class,
                () -> bank.collectItem(new BankDataCollector.Item("overflow", "A", "B", 1)));
        assertEquals(10, bank.getBalance("A"));
        assertEquals(Long.MAX_VALUE, bank.getBalance("B"));
        assertEquals(0, bank.getProcessedCount());
        assertThrows(IllegalArgumentException.class,
                () -> new BankDataCollector.Item("bad", "A", "A", 1));
        assertThrows(IllegalArgumentException.class, () -> bank.deposit("A", -1));
        assertThrows(IllegalArgumentException.class,
                () -> bank.collectItem(new BankDataCollector.Item("bad", "A", "missing", 1)));
        assertTrue(bank.collectItem(new BankDataCollector.Item("key", "B", "A", 1)));
        assertThrows(IllegalArgumentException.class,
                () -> bank.collectItem(new BankDataCollector.Item("key", "B", "A", 2)));
    }

    private void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.getState() != Thread.State.TIMED_WAITING) {
            if (!thread.isAlive() || System.nanoTime() - deadline >= 0) {
                fail("Worker did not enter timed wait: " + thread.getState());
            }
            Thread.sleep(1);
        }
    }
}
