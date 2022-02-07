package com.algotrading.connectors.quik;

import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.SimpleLogger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class QuikConnectTest {

    private static final AbstractLogger LOGGER = new SimpleLogger();

    public static void main(final String[] args) {
        LOGGER.info("STARTED");
        final TestListener testListener = new TestListener();
        final QuikConnect quikConnect = new QuikConnect(
                "localhost", 10001, 10002, "test", testListener
        );
        LOGGER.info("Starting QuikConnect");
        quikConnect.start();
        testListener.run(quikConnect);
        LOGGER.info("Shutting down QuikConnect");
        quikConnect.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    static class TestListener implements QuikListener {
        private final ZonedDateTime deadline = ZonedDateTime.now().plusSeconds(10);
        private volatile boolean isOpen = false;

        @Override
        public void onOpen() {
            LOGGER.info("OnOpen");
            isOpen = true;
        }

        @Override
        public void onClose() {
            LOGGER.info("OnClose");
            isOpen = false;
        }

        @Override
        public void onCallback(final JSONObject jsonObject) {
            LOGGER.info("OnCallBack " + jsonObject.get("callback"));
            LOGGER.info(jsonObject.toString());
        }

        @Override
        public void onExceptionMN(final Exception exception) {
            LOGGER.log(AbstractLogger.ERROR, "onExceptionMN", exception);
        }

        @Override
        public void onExceptionCB(final Exception exception) {
            LOGGER.log(AbstractLogger.ERROR, "onExceptionCB", exception);
        }

        private static void waitFor(final CompletableFuture<JSONObject> cf) throws InterruptedException, ExecutionException {
            long t = System.nanoTime();
            final JSONObject json = cf.get();
            t = (System.nanoTime() - t) / 1_000_000L;
            LOGGER.info(t + " ms; " + json);
        }

        @SuppressWarnings("BusyWait")
        public void run(final QuikConnect quikConnect) {
            boolean isInitialized = false;
            while (ZonedDateTime.now().isBefore(deadline)) {
                if (!isOpen) {
                    try {
                        Thread.sleep(10L);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    continue;
                }
                try {
                    if (!isInitialized) {
                        LOGGER.info("Correct requests:");
                        waitFor(quikConnect.getResponseMN("message(\"Hello, QLua-world!\", 2)", 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseCB("return os.sysdate()", 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("os.sysdate", null, 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseCB("os.sysdate", List.of(), 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("isConnected", null, 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("math.max", List.of(1, 3, 5, 7), 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("message", List.of("Hi, there!", 1), 1, TimeUnit.SECONDS));

                        LOGGER.info("Erroneous requests:");
                        waitFor(quikConnect.getResponseMN("return string(((", 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("return math.max(\"ABC\", 15)", 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("mess", null, 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseMN("math.max", List.of("ABC", 15), 1, TimeUnit.SECONDS));

                        LOGGER.info("Correct requests:");
                        waitFor(quikConnect.getResponseMN("return initDataSource(\"TQBR\", \"AFLT\", 1)", 1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseCB("OnAllTrade",
                                "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                                1, TimeUnit.SECONDS));
                        waitFor(quikConnect.getResponseCB("OnCandle",
                                "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                                1, TimeUnit.SECONDS));
                        isInitialized = true;
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (final ExecutionException e) {
                    LOGGER.log(AbstractLogger.ERROR, e.getMessage(), e);
                }
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
