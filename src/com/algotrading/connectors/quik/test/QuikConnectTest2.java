package com.algotrading.connectors.quik.test;

import com.algotrading.base.util.TimeConditionTrigger;
import com.algotrading.connectors.quik.QuikConnect;
import com.algotrading.connectors.quik.QuikServerConnectionStatus;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.logs.SimpleLogger;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

import static com.algotrading.connectors.quik.AbstractQuikListener.pause;

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
        LOGGER.info("Starting execution thread");
        connectionStatus.getExecutionThread().start();
        runUntil(ZonedDateTime.now().plus(duration, unit));
    }

    private void shutdown() {
        LOGGER.info("Shutting down QuikConnect");
        quikConnect.shutdown();
    }

    private void runUntil(final ZonedDateTime deadline) {
        final TimeConditionTrigger trigger = TimeConditionTrigger.newSecondChangedTrigger();
        while (deadline.isAfter(ZonedDateTime.now())) {
            if (trigger.triggered()) {
                LOGGER.info("Connected since: " + connectionStatus.connectedSince());
            }
            pause(10);
        }
        connectionStatus.getExecutionThread().interrupt();
        try {
            connectionStatus.getExecutionThread().join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
