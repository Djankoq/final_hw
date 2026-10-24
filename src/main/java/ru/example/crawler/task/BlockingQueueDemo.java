package ru.example.crawler.task;

import ru.example.crawler.service.UrlTaskServer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/** Несколько производителей URL и потребителей, скачивающих заголовки страниц. */
public final class BlockingQueueDemo {
    private BlockingQueueDemo() { }

    public static void main(String[] args) throws Exception {
        List<String> urls;
        if (args.length == 0) {
            urls = List.of("https://example.com", "https://www.wikipedia.org",
                    "https://www.oracle.com/java/", "https://openjdk.org",
                    "https://spring.io", "https://example.org");
        } else if (args.length == 2 && "--file".equals(args[0])) {
            urls = Files.readAllLines(Path.of(args[1]), StandardCharsets.UTF_8).stream()
                    .map(String::strip)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .toList();
        } else if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--"))) {
            throw new IllegalArgumentException("Использование: [URL ...] или --file urls.txt");
        } else {
            urls = List.of(args);
        }

        System.out.println("BlockingQueue: производителей — 2, потребителей — 3, ёмкость очереди — 8");
        List<UrlTaskServer.Result> results = new UrlTaskServer().process(urls);
        for (UrlTaskServer.Result result : results) {
            System.out.printf("[%s] %s | HTTP %s | %s%n", result.consumer(), result.url(),
                    result.statusCode() == null ? "—" : result.statusCode(),
                    result.successful() ? result.title() : "Ошибка: " + result.error());
        }
        long successes = results.stream().filter(UrlTaskServer.Result::successful).count();
        System.out.printf("Всего: %d; заголовков: %d; ошибок: %d%n",
                results.size(), successes, results.size() - successes);
    }
}
