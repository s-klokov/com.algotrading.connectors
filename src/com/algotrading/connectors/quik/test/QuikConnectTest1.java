package com.algotrading.connectors.quik.test;

import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.SimpleLogger;
import com.algotrading.connectors.quik.QuikConnect;
import com.algotrading.connectors.quik.QuikListener;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Тестирование подключения к терминалу QUIK.
 */
class QuikConnectTest1 {

    private static final AbstractLogger LOGGER = new SimpleLogger();
    private QuikConnect quikConnect = null;
    private QuikListener quikListener = null;

    public static void main(final String[] args) {
        LOGGER.info("STARTED");
        final QuikConnectTest1 test = new QuikConnectTest1();
        test.init();
        test.run(120, ChronoUnit.SECONDS);
        test.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    private void init() {
        quikListener = new TestListener();
        quikConnect = new QuikConnect("localhost", 10001, 10002, "test", quikListener);
        quikListener.setQuikConnect(quikConnect);
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
        while (quikListener.isRunning()) {
            quikListener.step(Thread.currentThread().isInterrupted() || !ZonedDateTime.now().isBefore(deadline));
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class TestListener implements QuikListener {
        private QuikConnect quikConnect = null;
        private boolean isRunning = true;
        private boolean areRequestsDone = false;
        private volatile boolean isOpen = false;

        @Override
        public void setQuikConnect(final QuikConnect quikConnect) {
            this.quikConnect = quikConnect;
        }

        @Override
        public boolean isRunning() {
            return isRunning;
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

        @Override
        public void step(final boolean isInterrupted) {
            if (!isRunning()) {
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

        private void doRequests(final QuikConnect quikConnect) {
            try {
                LOGGER.info("Correct requests:");
                waitFor(quikConnect.futureResponseMN("message(\"Hello, QLua-world!\", 2)", 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseCB("return os.sysdate()", 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("os.sysdate", null, 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseCB("os.sysdate", List.of(), 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("isConnected", null, 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("math.max", List.of(1, 3, 5, 7), 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("message", List.of("Hi, there!", 1), 5, TimeUnit.SECONDS));

                LOGGER.info("Erroneous requests:");
                waitFor(quikConnect.futureResponseMN("return string(((", 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("return math.max(\"ABC\", 15)", 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("mess", null, 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseMN("math.max", List.of("ABC", 15), 5, TimeUnit.SECONDS));

                LOGGER.info("Correct requests:");
                waitFor(quikConnect.futureResponseMN("return initDataSource(\"TQBR\", \"AFLT\", 1)", 5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseCB("OnAllTrade",
                        "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                        5, TimeUnit.SECONDS));
                waitFor(quikConnect.futureResponseCB("OnCandle",
                        "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                        5, TimeUnit.SECONDS));
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
