package com.algotrading.connectors.quik.test;

import com.algotrading.connectors.quik.AbstractQuikListener;
import com.algotrading.connectors.quik.QuikConnect;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.logs.SimpleLogger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestListener extends AbstractQuikListener {

    private static final AbstractLogger LOGGER = new SimpleLogger();
    private boolean isOpen = false;

    public TestListener() {
        executionThread = Thread.currentThread();
    }

    @Override
    public void onOpen() {
        submit(() -> {
            LOGGER.debug("onOpen");
            isOpen = true;
        });
    }

    @Override
    public void onClose() {
        submit(() -> {
            LOGGER.debug("onClose");
            isOpen = false;
        });
    }

    @Override
    public void onCallback(final JSONObject jsonObject) {
        submit(() -> {
            LOGGER.debug("onCallback " + jsonObject.get("callback"));
            LOGGER.debug(jsonObject.toString());
        });
    }

    @Override
    public void onExceptionMN(final Exception exception) {
        submit(() -> LOGGER.log(AbstractLogger.ERROR, "onExceptionMN", exception));
    }

    @Override
    public void onExceptionCB(final Exception exception) {
        submit(() -> LOGGER.log(AbstractLogger.ERROR, "onExceptionCB", exception));
    }

    private void processRunnables() {
        Runnable runnable;
        while ((runnable = queue.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                LOGGER.log(AbstractLogger.ERROR, "Cannot execute a runnable from QuikListener", e);
            }
        }
    }

    public static void main(final String[] args) {
        final TestListener testListener = new TestListener();
        final QuikConnect quikConnect = new QuikConnect("192.168.15.19", 10001, 10002, "test", testListener);
        testListener.setQuikConnect(quikConnect);
        quikConnect.start();
        final ZonedDateTime stoppingTime = ZonedDateTime.now().plus(1, ChronoUnit.HOURS);
        int counter = 0;
        final StringBuilder sb = new StringBuilder();
        while (ZonedDateTime.now().isBefore(stoppingTime)) {
            testListener.processRunnables();
            if (testListener.isOpen) {
                sb.setLength(0);
                sb.append(++counter).append(": ");
                try {
                    JSONObject json;
                    json = quikConnect.executeCB("isConnected", (List<?>) null, 5, TimeUnit.SECONDS);
                    sb.append(json.get("result")).append("; ");
                    json = quikConnect.executeMN("initDataSource", List.of("TQBR", "SBER", 5), 5, TimeUnit.SECONDS);
                    sb.append(json.get("result")).append("; ");
                    json = quikConnect.executeCB("getDataSourceSize", List.of("TQBR", "SBER", 5), 5, TimeUnit.SECONDS);
                    sb.append(json.get("result")).append("; ");
                    json = quikConnect.executeMN("getCandles", List.of("TQBR", "SBER", 5, 3), 5, TimeUnit.SECONDS);
                    sb.append(json.get("result")).append("; ");
                    json = quikConnect.executeCB("OnAllTrade", "*", 5, TimeUnit.SECONDS);
                    sb.append(json.get("result")).append("; ");
                    LOGGER.info(sb.toString());
                    //pause(250L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (final Exception e) {
                    LOGGER.log(AbstractLogger.ERROR, "Exception", e);
                    pause(1000L);
                }
            } else {
                pause(1000L);
            }
        }
        quikConnect.shutdown();
    }
}
