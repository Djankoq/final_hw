package ru.example.crawler.task;

/** Одноразовый сигнал кооперативной остановки. */
public final class StopFlag {
    private volatile boolean stopped;

    public void requestStop() {
        stopped = true;
    }

    public boolean isStopped() {
        return stopped;
    }
}
