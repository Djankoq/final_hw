package ru.example.crawler.service;

import org.springframework.stereotype.Service;

/**
 * Точка расширения будущего краулера: получение страниц, обход ссылок
 * и извлечение открытых контактных данных.
 */
@Service
public class ContactCrawlerService {

    public String describe() {
        return "ContactCrawlerService готов к обработке стартовых URL.";
    }
}
