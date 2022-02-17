package com.algotrading.connectors.quik.test;

import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.SimpleLogger;
import com.algotrading.base.util.TimeConditionTrigger;
import com.algotrading.connectors.quik.QuikConnect;
import com.algotrading.connectors.quik.QuikServerConnectionStatus;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

/**
 * Тестирование подключения к терминалу QUIK.
 */
class QuikConnectTest2 {

    private static final AbstractLogger LOGGER = new SimpleLogger();
    private QuikConnect quikConnect = null;
    private QuikServerConnectionStatus connectionStatus = null;

    public static void main(final String[] args) {
        LOGGER.info("STARTED");
        final QuikConnectTest2 test = new QuikConnectTest2();
        test.init();
        test.run(5, ChronoUnit.MINUTES);
        test.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    private void init() {
        final String clientId = "test";
        connectionStatus = new QuikServerConnectionStatus(LOGGER, clientId);
        quikConnect = new QuikConnect("localhost", 10001, 10002, clientId, connectionStatus);
        connectionStatus.setQuikConnect(quikConnect);
    }

    private void run(final long duration, final TemporalUnit unit) {
        LOGGER.info("Starting QuikConnect");
        quikConnect.start();
        runUntil(ZonedDateTime.now().plus(duration, unit));
    }

    private void shutdown() {
        LOGGER.info("Shutting down QuikConnect");
        quikConnect.shutdown();
    }

    @SuppressWarnings("BusyWait")
    private void runUntil(final ZonedDateTime deadline) {
        final TimeConditionTrigger trigger = TimeConditionTrigger.newSecondChangedTrigger();
        while (connectionStatus.isRunning()) {
            connectionStatus.step(Thread.currentThread().isInterrupted() || !ZonedDateTime.now().isBefore(deadline));
            if (trigger.triggered()) {
                LOGGER.info("Connected since: " + connectionStatus.getConnectedSince());
            }
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
