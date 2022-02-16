package com.algotrading.connectors.quik;

import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.SimpleLogger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class QuikConnectTest {

    private static final AbstractLogger LOGGER = new SimpleLogger();
    private QuikConnect quikConnect = null;
    private TestListener testListener = null;

    public static void main(final String[] args) {
        LOGGER.info("STARTED");
        final QuikConnectTest test = new QuikConnectTest();
        test.init();
        test.run(120, ChronoUnit.SECONDS);
        test.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    private void init() {
        testListener = new TestListener();
        quikConnect = new QuikConnect("localhost", 10001, 10002, "test", testListener);
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
        while (testListener.isRunning()) {
            testListener.step(quikConnect, Thread.currentThread().isInterrupted() || !ZonedDateTime.now().isBefore(deadline));
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class TestListener implements QuikListener {
        private boolean isRunning = true;
        private boolean areRequestsDone = false;
        private volatile boolean isOpen = false;

        @Override
        public boolean isRunning() {
            return isRunning;
        }

        @Override
        public void step(final QuikConnect quikConnect, final boolean isInterrupted) {
            if (!isRunning) {
                return;
            }
            if (isOpen && !areRequestsDone && !quikConnect.hasErrorMN() && !quikConnect.hasErrorCB()) {
                doRequests(quikConnect);
                areRequestsDone = true;
            }
            if (isInterrupted) {
                isRunning = false;
            }
        }

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

        private void doRequests(final QuikConnect quikConnect) {
            try {
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
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (final ExecutionException e) {
                LOGGER.log(AbstractLogger.ERROR, e.getMessage(), e);
            }
        }

        private static void waitFor(final CompletableFuture<JSONObject> cf) throws InterruptedException, ExecutionException {
            long t = System.nanoTime();
            final JSONObject json = cf.get();
            t = (System.nanoTime() - t) / 1_000_000L;
            LOGGER.info(t + " ms; " + json);
        }
    }
}
