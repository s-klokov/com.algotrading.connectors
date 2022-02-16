package com.algotrading.connectors.quik;

import com.algotrading.base.util.TimeConditionTrigger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Отслеживание наличия подключения терминала QUIK к серверу QUIK.
 */
public class QuikServerConnectionStatus implements QuikListener {
    private QuikConnect quikConnect = null;
    private volatile boolean isRunning = true;
    private final Queue<Runnable> queue = new LinkedBlockingDeque<>();
    private volatile ZonedDateTime connectedSince = null;

    private CompletableFuture<Boolean> isConnected = null;
    private boolean isSubscribed = false;
    private TimeConditionTrigger checkTrigger = null;

    @Override
    public void setQuikConnect(final QuikConnect quikConnect) {
        this.quikConnect = quikConnect;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    public ZonedDateTime getConnectedSince() {
        return connectedSince;
    }

    private void executeAtNextStep(final Runnable runnable) {
        queue.add(runnable);
    }

    private void executeRunnables() {
        Runnable runnable;
        while ((runnable = queue.poll()) != null) {
            runnable.run();
        }
    }

    private void reset() {
        // TODO:
    }

    @Override
    public void onOpen() {
        if (!isRunning()) {
            return;
        }
        executeAtNextStep(() -> {
            isConnected = null;// TODO:
        });
    }

    @Override
    public void onClose() {
        if (!isRunning()) {
            return;
        }
        executeAtNextStep(this::reset);
    }

    @Override
    public void onCallback(final JSONObject jsonObject) {
        if (!isRunning()) {
            return;
        }
        if ("OnDisconnected".equals(jsonObject.get("callback"))) {
            executeAtNextStep(() -> {
                // TODO:
            });
        }
    }

    @Override
    public void onExceptionMN(final Exception exception) {
        if (!isRunning()) {
            return;
        }
        executeAtNextStep(this::reset);
    }

    @Override
    public void onExceptionCB(final Exception exception) {
        if (!isRunning()) {
            return;
        }
        executeAtNextStep(this::reset);
    }

    @Override
    public void step(final boolean isInterrupted) {
        if (!isRunning()) {
            return;
        }
        if (isInterrupted) {
            isRunning = false;
        }
        executeRunnables();
        // TODO:
        if (isInterrupted) {
            // TODO:
            connectedSince = null;
        }
    }
}
