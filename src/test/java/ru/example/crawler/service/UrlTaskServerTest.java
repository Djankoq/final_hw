package ru.example.crawler.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class UrlTaskServerTest {
    private HttpServer server;
    private ExecutorService serverExecutor;
    private String baseUrl;
    private final AtomicInteger pageRequests = new AtomicInteger();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.createContext("/page", exchange -> {
            pageRequests.incrementAndGet();
            respond(exchange, 200, "<title>Страница " + exchange.getRequestURI().getRawQuery() + "</title>");
        });
        server.createContext("/html", exchange -> respond(exchange, 200, """
                <html><head>
                <!-- <title>Заголовок в комментарии</title> -->
                <script>const text = '<title>Заголовок в скрипте</title>';</script>
                <TiTlE>  Реальный &amp; &#65; &lt;текст&gt; &nbsp;
                    заголовок  </TiTlE>
                </head><body>Страница</body></html>
                """));
        server.createContext("/no-title", exchange -> respond(exchange, 200, "<html><body>Без заголовка</body></html>"));
        server.createContext("/blank-title", exchange -> respond(exchange, 200, "<title> \n </title>"));
        server.createContext("/error", exchange -> respond(exchange, 503, "<title>Unavailable</title>"));
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() throws InterruptedException {
        server.stop(0);
        serverExecutor.shutdownNow();
        assertTrue(serverExecutor.awaitTermination(3, TimeUnit.SECONDS));
    }

    @Test
    void processesEveryOccurrenceWithMultipleProducersAndASingleSlotQueue() throws Exception {
        var taskServer = new UrlTaskServer(4, 3, 1, Duration.ofSeconds(3));
        List<String> urls = IntStream.range(0, 80)
                .mapToObj(i -> baseUrl + "/page?" + (i % 17)).toList();

        var results = taskServer.process(urls);

        assertEquals(urls, results.stream().map(UrlTaskServer.Result::url).toList());
        assertEquals(80, pageRequests.get(), "Каждое вхождение URL должно загружаться ровно один раз");
        for (int i = 0; i < results.size(); i++) {
            var result = results.get(i);
            assertTrue(result.successful(), () -> "Неожиданная ошибка: " + result.error());
            assertEquals(200, result.statusCode());
            assertEquals("Страница " + (i % 17), result.title());
            assertTrue(result.consumer().startsWith("url-consumer-"));
        }
        assertThrows(UnsupportedOperationException.class, results::clear);
        assertNoWorkersAlive();
    }

    @Test
    void consumersDownloadPagesConcurrently() throws Exception {
        CountDownLatch entered = new CountDownLatch(3);
        CountDownLatch release = new CountDownLatch(1);
        createBlockedPage("/parallel", entered, release);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            var taskServer = new UrlTaskServer(2, 3, 1, Duration.ofSeconds(5));
            var future = caller.submit(() -> taskServer.process(List.of(
                    baseUrl + "/parallel", baseUrl + "/parallel", baseUrl + "/parallel")));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS), "Три потребителя должны одновременно начать загрузку");
                assertFalse(future.isDone());
            } finally {
                release.countDown();
            }
            var results = future.get(5, TimeUnit.SECONDS);
            assertTrue(results.stream().allMatch(UrlTaskServer.Result::successful));
            assertEquals(3, results.stream().map(UrlTaskServer.Result::consumer).distinct().count());
        } finally {
            release.countDown();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
        }
        assertNoWorkersAlive();
    }

    @Test
    void reportsBadUrlsHttpErrorsAndMissingTitlesThenContinuesOnTheSameConsumer() throws Exception {
        var taskServer = new UrlTaskServer(2, 1, 1, Duration.ofSeconds(3));
        List<String> urls = List.of("invalid", "file:///tmp/page.html", "ftp://example.com",
                baseUrl + "/error", baseUrl + "/no-title", baseUrl + "/blank-title",
                baseUrl + "/html", baseUrl + "/page?last");

        var results = taskServer.process(urls);

        assertEquals(urls, results.stream().map(UrlTaskServer.Result::url).toList());
        for (int i = 0; i < 6; i++) {
            assertFalse(results.get(i).successful());
            assertNotNull(results.get(i).error());
            assertNull(results.get(i).title());
        }
        assertNull(results.get(0).statusCode());
        assertEquals(503, results.get(3).statusCode());
        assertEquals("HTTP 503", results.get(3).error());
        assertEquals(200, results.get(4).statusCode());
        assertEquals("Реальный & A <текст> заголовок", results.get(6).title());
        assertTrue(results.get(6).successful());
        assertEquals("Страница last", results.get(7).title());
        assertEquals(1, results.stream().map(UrlTaskServer.Result::consumer).distinct().count());
    }

    @Test
    void followsRedirectsAndUsesTheResponseCharset() throws Exception {
        server.createContext("/encoded", exchange -> respond(exchange, 200,
                "<title>Заголовок в другой кодировке</title>", Charset.forName("windows-1251")));
        server.createContext("/redirect", exchange -> {
            try {
                exchange.getResponseHeaders().set("Location", "/encoded");
                exchange.sendResponseHeaders(302, -1);
            } finally {
                exchange.close();
            }
        });

        var result = new UrlTaskServer().process(List.of(baseUrl + "/redirect")).get(0);

        assertTrue(result.successful(), result.error());
        assertEquals(baseUrl + "/redirect", result.url());
        assertEquals(200, result.statusCode());
        assertEquals("Заголовок в другой кодировке", result.title());
    }

    @Test
    void timesOutAndProcessesTheNextUrl() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        createBlockedPage("/slow", new CountDownLatch(0), release);
        try {
            var taskServer = new UrlTaskServer(2, 1, 1, Duration.ofMillis(500));
            var results = taskServer.process(List.of(baseUrl + "/slow", baseUrl + "/page?after-timeout"));

            assertFalse(results.get(0).successful());
            assertNull(results.get(0).statusCode());
            assertTrue(results.get(0).error().contains("HttpTimeoutException"));
            assertTrue(results.get(1).successful(), results.get(1).error());
            assertEquals("Страница after-timeout", results.get(1).title());
        } finally {
            release.countDown();
        }
        assertNoWorkersAlive();
    }

    @Test
    void finishesEmptyAndShortInputsAndAllowsReuse() throws Exception {
        var taskServer = new UrlTaskServer(4, 5, 1, Duration.ofSeconds(3));

        assertTrue(taskServer.process(List.of()).isEmpty());
        assertEquals("Страница first", taskServer.process(List.of(baseUrl + "/page?first")).get(0).title());
        assertEquals("Страница second", taskServer.process(List.of(baseUrl + "/page?second")).get(0).title());
        assertNoWorkersAlive();
    }

    @Test
    void interruptionStopsProducersWaitingForSpaceAndConsumerWaitingForHttp() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        createBlockedPage("/blocked", entered, release);
        var taskServer = new UrlTaskServer(2, 1, 1, Duration.ofSeconds(30));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                taskServer.process(IntStream.range(0, 100).mapToObj(i -> baseUrl + "/blocked").toList());
            } catch (Throwable caught) {
                failure.set(caught);
            }
        }, "test-url-coordinator");
        caller.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(5000);
            assertFalse(caller.isAlive(), "Координатор должен завершиться после прерывания");
            assertInstanceOf(InterruptedException.class, failure.get());
            assertNoWorkersAlive();
        } finally {
            release.countDown();
            caller.interrupt();
            caller.join(5000);
        }
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new UrlTaskServer(0, 1, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new UrlTaskServer(1, 0, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new UrlTaskServer(1, 1, 0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new UrlTaskServer(1, 1, 1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new UrlTaskServer(1, 1, 1, Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> new UrlTaskServer(1, 1, 1, null));
    }

    private void createBlockedPage(String path, CountDownLatch entered, CountDownLatch release) {
        server.createContext(path, exchange -> {
            entered.countDown();
            try {
                if (release.await(5, TimeUnit.SECONDS)) respond(exchange, 200, "<title>Готово</title>");
            } catch (InterruptedException interruption) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
    }

    private static void respond(HttpExchange exchange, int status, String html) throws IOException {
        respond(exchange, status, html, StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, int status, String html, Charset charset) throws IOException {
        try {
            byte[] body = html.getBytes(charset);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=" + charset.name());
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        } finally {
            exchange.close();
        }
    }

    private static void assertNoWorkersAlive() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        assertTrue(threads.stream().noneMatch(thread -> thread.isAlive()
                        && (thread.getName().startsWith("url-producer-") || thread.getName().startsWith("url-consumer-"))),
                "Все производители и потребители должны завершиться");
    }
}
