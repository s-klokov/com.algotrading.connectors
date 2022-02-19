package com.algotrading.connectors.quik;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class AbstractQuikListener implements QuikListener {
    /**
     * Подключение к терминалу QUIK.
     */
    protected volatile QuikConnect quikConnect = null;
    /**
     * Поток для исполнения бизнес-логики.
     */
    protected volatile Thread executionThread = null;
    /**
     * Очередь на исполнение.
     */
    protected final Queue<Runnable> queue = new LinkedBlockingDeque<>();

    @Override
    public void setQuikConnect(final QuikConnect quikConnect) {
        this.quikConnect = quikConnect;
    }

    @Override
    public Thread getExecutionThread() {
        return executionThread;
    }

    @Override
    public void execute(final Runnable runnable) {
        queue.add(runnable);
    }

    /**
     * Сделать паузу указанной длительности.
     *
     * @param millis длительность паузы в миллисекундах
     */
    protected void pause(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
