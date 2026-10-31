package ru.example.crawler.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ru.example.crawler.task.VirtualThreadsDemo.Mode;

@Timeout(15)
class VirtualThreadsDemoTest {
    private final VirtualThreadsDemo demo = new VirtualThreadsDemo();

    static Stream<Arguments> scenarios() {
        return Arrays.stream(Mode.values()).flatMap(mode -> IntStream.of(1, 100, 1_000)
                .mapToObj(count -> Arguments.of(mode, count)));
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void everyTaskRunsExactlyOnceInTheRequestedThreadType(Mode mode, int count) throws Exception {
        AtomicIntegerArray visits = new AtomicIntegerArray(count);
        var workers = new ConcurrentLinkedQueue<Thread>();
        var result = demo.measure(mode, count, id -> {
            Thread worker = Thread.currentThread();
            workers.add(worker);
            assertEquals(mode != Mode.PLATFORM, worker.isVirtual());
            visits.incrementAndGet(id);
            Thread.sleep(2);
        });
        assertEquals(count, result.completed());
        assertEquals(count, workers.size());
        assertEquals(count, workers.stream().distinct().count());
        for (int id = 0; id < count; id++) {
            assertEquals(1, visits.get(id));
        }
        assertTrue(workers.stream().noneMatch(Thread::isAlive));
        assertTrue(result.elapsedNanos() > 0);
        assertTrue(result.processCpuNanos() >= -1);
        assertTrue(result.sampledPeakHeapBytes() >= result.heapBeforeBytes());
        assertTrue(result.sampledPeakPlatformThreads() >= result.platformThreadsBefore());
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void simulatedIoPrintsEveryThreadName(Mode mode) throws Exception {
        var output = new ByteArrayOutputStream();
        try (var out = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            demo.measure(mode, 12, VirtualThreadsDemo.simulatedIoTask(1, out));
        }
        var names = output.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals(12, names.size());
        assertEquals(12, names.stream().distinct().count());
        assertTrue(names.stream().allMatch(name -> name.startsWith(mode.name().toLowerCase(Locale.ROOT) + "-")));
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void taskFailureIsPropagatedAndNoWorkersRemain(Mode mode) {
        IOException failure = new IOException("test failure");
        var workers = new ConcurrentLinkedQueue<Thread>();
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> demo.measure(mode, 32, id -> {
                    workers.add(Thread.currentThread());
                    throw failure;
                }));
        assertSame(failure, error.getCause());
        assertFalse(workers.isEmpty());
        assertTrue(workers.stream().noneMatch(Thread::isAlive));
    }

    @Test
    void structuredFailureInterruptsSleepingSiblingsBeforeReturning() {
        CountDownLatch sleeping = new CountDownLatch(2);
        AtomicInteger cancelled = new AtomicInteger();
        var workers = new ConcurrentLinkedQueue<Thread>();
        IOException failure = new IOException("source unavailable");
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> demo.measure(Mode.STRUCTURED, 3, id -> {
                    workers.add(Thread.currentThread());
                    if (id == 2) {
                        assertTrue(sleeping.await(3, TimeUnit.SECONDS));
                        throw failure;
                    }
                    sleeping.countDown();
                    try {
                        Thread.sleep(60_000);
                    } catch (InterruptedException interrupted) {
                        cancelled.incrementAndGet();
                        throw interrupted;
                    }
                }));
        assertSame(failure, error.getCause());
        assertEquals(2, cancelled.get());
        assertTrue(workers.stream().noneMatch(Thread::isAlive));
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void interruptingOwnerCancelsAndJoinsAllWorkers(Mode mode) throws Exception {
        CountDownLatch started = new CountDownLatch(8);
        var workers = new ConcurrentLinkedQueue<Thread>();
        var outcome = new FutureTask<>(() -> assertThrows(InterruptedException.class,
                () -> demo.measure(mode, 8, id -> {
                    workers.add(Thread.currentThread());
                    started.countDown();
                    Thread.sleep(60_000);
                })));
        Thread owner = new Thread(outcome, "comparison-test-owner");
        owner.start();
        try {
            assertTrue(started.await(3, TimeUnit.SECONDS));
            owner.interrupt();
            assertNotNull(outcome.get(3, TimeUnit.SECONDS));
            assertEquals(8, workers.size());
            assertTrue(workers.stream().noneMatch(Thread::isAlive));
        } finally {
            owner.interrupt();
            owner.join(3_000);
        }
        assertFalse(owner.isAlive());
    }

    @Test
    void invalidParametersAreRejectedBeforeStartingTasks() {
        assertThrows(IllegalArgumentException.class, () -> demo.measure(Mode.VIRTUAL, 0, id -> fail()));
        assertThrows(IllegalArgumentException.class, () -> demo.measure(Mode.PLATFORM, -1, id -> fail()));
        assertThrows(NullPointerException.class, () -> demo.measure(null, 10, id -> fail()));
        assertThrows(NullPointerException.class, () -> demo.measure(Mode.STRUCTURED, 10, null));
        assertThrows(IllegalArgumentException.class, () -> VirtualThreadsDemo.simulatedIoTask(-1, null));
        assertThrows(IllegalArgumentException.class, () -> VirtualThreadsDemo.main(new String[]{"--tasks"}));
        assertThrows(IllegalArgumentException.class,
                () -> VirtualThreadsDemo.main(new String[]{"--rounds", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> VirtualThreadsDemo.main(new String[]{"--tasks", "10,0"}));
    }
}
