package ru.example.crawler.task;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class HttpRequestsDemoTest {
    private HttpServer server;
    private ExecutorService serverExecutor;
    private String baseUrl;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.createContext("/ok", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/missing", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        serverExecutor.shutdownNow();
    }

    @Test
    void collectsStatusesErrorsAndSummaryInInputOrder() throws Exception {
        try (var demo = new HttpRequestsDemo()) {
            List<String> urls = List.of(baseUrl + "/ok", "invalid", baseUrl + "/missing", baseUrl + "/ok");
            var summary = demo.fetchAll(urls).get(5, TimeUnit.SECONDS);
            assertEquals(urls, summary.results().stream().map(HttpRequestsDemo.Result::url).toList());
            assertEquals(200, summary.results().get(0).statusCode());
            assertTrue(summary.results().get(0).successful());
            assertNull(summary.results().get(1).statusCode());
            assertNotNull(summary.results().get(1).error());
            assertEquals(404, summary.results().get(2).statusCode());
            assertFalse(summary.results().get(2).successful());
            assertTrue(summary.results().stream().allMatch(result -> !result.elapsed().isNegative()));
            var output = new ByteArrayOutputStream();
            summary.print(new PrintStream(output, true, java.nio.charset.StandardCharsets.UTF_8));
            String text = output.toString(java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(text.contains("Всего: 4; успешных (2xx): 2; HTTP вне 2xx: 1; ошибок: 1"));
            assertTrue(text.contains("{200=2, 404=1}"));
        }
    }

    @Test
    void runsRequestsConcurrentlyAndReturnsBeforeCompletion() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext("/parallel", exchange -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
                exchange.sendResponseHeaders(200, -1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        try (var demo = new HttpRequestsDemo(2, 2, 2, Duration.ofSeconds(5))) {
            var future = demo.fetchAll(List.of(baseUrl + "/parallel", baseUrl + "/parallel"));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS), "Оба запроса должны начаться до завершения первого");
                assertFalse(future.isDone());
            } finally {
                release.countDown();
            }
            assertTrue(future.get(5, TimeUnit.SECONDS).results().stream().allMatch(HttpRequestsDemo.Result::successful));
        }
    }

    @Test
    void timeoutDoesNotPreventOtherResults() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        server.createContext("/slow", exchange -> {
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        try (var demo = new HttpRequestsDemo(2, 2, 2, Duration.ofMillis(500))) {
            var results = demo.fetchAll(List.of(baseUrl + "/slow", baseUrl + "/ok"))
                    .get(5, TimeUnit.SECONDS).results();
            assertNull(results.get(0).statusCode());
            assertTrue(results.get(0).error().contains("HttpTimeoutException"));
            assertEquals(200, results.get(1).statusCode());
        } finally {
            release.countDown();
        }
    }

    @Test
    void reportsQueueOverflowWithoutLosingAcceptedRequests() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext("/blocked", exchange -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
                exchange.sendResponseHeaders(200, -1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        try (var demo = new HttpRequestsDemo(1, 1, 1, Duration.ofSeconds(5))) {
            var active = demo.fetchAll(List.of(baseUrl + "/blocked"));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                var remaining = demo.fetchAll(List.of(baseUrl + "/ok", baseUrl + "/missing"));
                release.countDown();
                var results = remaining.get(5, TimeUnit.SECONDS).results();
                assertEquals(200, results.get(0).statusCode());
                assertNull(results.get(1).statusCode());
                assertTrue(results.get(1).error().contains("переполнена"));
                assertEquals(200, active.get(5, TimeUnit.SECONDS).results().get(0).statusCode());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void handlesEmptyInputAndClosedPool() throws Exception {
        var demo = new HttpRequestsDemo();
        assertTrue(demo.fetchAll(List.of()).get().results().isEmpty());
        demo.close();
        var result = demo.fetchAll(List.of(baseUrl + "/ok")).get().results().get(0);
        assertNull(result.statusCode());
        assertNotNull(result.error());
    }
}
