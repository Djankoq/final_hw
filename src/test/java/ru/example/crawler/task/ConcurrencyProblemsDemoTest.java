package ru.example.crawler.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(25)
class ConcurrencyProblemsDemoTest {
    @TempDir
    Path tempDirectory;

    @Test
    void deadlockIsConfirmedInSeparateJvmAndDoesNotTerminate() throws Exception {
        Path output = tempDirectory.resolve("deadlock.txt");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classes = Path.of(ConcurrencyProblemsDemo.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).toString();
        Process process = new ProcessBuilder(java, "-cp", classes,
                ConcurrencyProblemsDemo.class.getName(), "deadlock")
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            String console;
            do {
                console = Files.readString(output);
                if (console.contains("DEADLOCK CONFIRMED")) {
                    break;
                }
                assertTrue(process.isAlive(), console);
                Thread.sleep(10);
            } while (System.nanoTime() - deadline < 0);
            assertTrue(console.contains("Deadlock-1=BLOCKED, Deadlock-2=BLOCKED"), console);
            assertFalse(process.waitFor(200, TimeUnit.MILLISECONDS));
        } finally {
            process.destroyForcibly();
            assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Child JVM must be cleaned up");
        }
    }

    @Test
    void commonResourceOrderAllowsBothWorkersToFinish() throws Exception {
        assertEquals(2, ConcurrencyProblemsDemo.orderedLocks());
    }

    @Test
    void retryBudgetAndOrderedFallbackRestoreProgress() throws Exception {
        var stuck = ConcurrencyProblemsDemo.livelock(false);
        assertEquals(40, stuck.attempts());
        assertEquals(0, stuck.completed());
        var fixed = ConcurrencyProblemsDemo.livelock(true);
        assertEquals(40, fixed.attempts());
        assertEquals(2, fixed.completed());
    }

    @Test
    void fairAccessLetsBothWorkersCompleteTheirWork() throws Exception {
        var starved = ConcurrencyProblemsDemo.starvation();
        assertEquals(10_000, starved.highCompleted());
        assertEquals(0, starved.lowCompleted());
        var fair = ConcurrencyProblemsDemo.fairAccess();
        assertEquals(10_000, fair.highCompleted());
        assertEquals(10_000, fair.lowCompleted());
    }
}
