package ru.example.crawler.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(40)
class AtomicConcurrencyTest {
    @Test
    void concurrentIncrementsAndCacheAccessPreserveResults() throws Exception {
        var result = AtomicPerformanceDemo.run(new AtomicPerformanceDemo.AtomicState(), 8, false);
        assertEquals(1_600_000, result.operations());
        assertSame(SingletonCache.getInstance(), SingletonCache.getInstance());
        assertEquals("cached value", SingletonCache.getInstance().get());
    }

    @Test
    void volatileFlagStopsAllActiveWorkers() throws Exception {
        var state = new AtomicPerformanceDemo.AtomicState();
        var result = AtomicPerformanceDemo.run(state, 4, true);
        assertTrue(state.stopped());
        assertTrue(result.operations() >= 4);
        assertEquals(result.operations(), state.count());
    }

    @Test
    void stopBeforeStartPreventsWork() throws Exception {
        var flag = new StopFlag();
        var counter = new AtomicCounter();
        flag.requestStop();
        Thread worker = new Thread(() -> {
            while (!flag.isStopped()) {
                counter.increment();
            }
        });
        worker.setDaemon(true);
        worker.start();
        worker.join(5_000);
        assertFalse(worker.isAlive());
        assertEquals(0, counter.get());
    }
}
