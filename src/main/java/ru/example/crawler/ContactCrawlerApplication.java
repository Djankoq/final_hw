package ru.example.crawler;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.example.crawler.service.ContactCrawlerService;
import ru.example.crawler.task.CounterWorker;
import ru.example.crawler.task.LoggerTask;

@SpringBootApplication
public class ContactCrawlerApplication implements CommandLineRunner {

    private final ContactCrawlerService crawlerService;

    public ContactCrawlerApplication(ContactCrawlerService crawlerService) {
        this.crawlerService = crawlerService;
    }

    public static void main(String[] args) {
        SpringApplication.run(ContactCrawlerApplication.class, args);
    }

    @Override
    public void run(String... args) throws InterruptedException {
        System.out.println(crawlerService.describe());

        Thread counterWorker = new CounterWorker(5);
        Thread loggerThread = new Thread(new LoggerTask(5), "LoggerThread");

        counterWorker.start();
        loggerThread.start();

        printActiveThreads();

        counterWorker.join();
        loggerThread.join();
        System.out.println("Обе задачи завершены.");
    }

    private void printActiveThreads() {
        System.out.println("\nСписок активных потоков:");
        Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .sorted((left, right) -> left.getName().compareToIgnoreCase(right.getName()))
                .forEach(thread -> System.out.printf("- %s (id=%d, state=%s)%n",
                        thread.getName(), thread.getId(), thread.getState()));
        System.out.println();
    }
}
