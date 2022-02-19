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
import java.util.Queue;
import java.util.concurrent.*;

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
        test.run(10, ChronoUnit.SECONDS);
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
        LOGGER.info("Starting execution thread");
        quikListener.getExecutionThread().start();
        runUntil(ZonedDateTime.now().plus(duration, unit));
    }

    private void shutdown() {
        LOGGER.info("Shutting down execution thread");
        quikListener.getExecutionThread().interrupt();
        try {
            quikListener.getExecutionThread().join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Shutting down QuikConnect");
        quikConnect.shutdown();
    }

    private void runUntil(final ZonedDateTime deadline) {
        while (deadline.isAfter(ZonedDateTime.now())) {
            pause(10L);
        }
    }

    private static void pause(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static class TestListener implements QuikListener {
        private QuikConnect quikConnect = null;
        private final Thread executionThread;
        private final Queue<Runnable> queue = new LinkedBlockingDeque<>();
        private boolean areRequestsDone = false;
        private volatile boolean isOpen = false;

        TestListener() {
            executionThread = new Thread() {
                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted()) {
                        executeRunnables();
                        if (isOpen && !areRequestsDone && !quikConnect.hasErrorMN() && !quikConnect.hasErrorCB()) {
                            doRequests(quikConnect);
                            areRequestsDone = true;
                        }
                        pause(100L);
                    }
                    LOGGER.info("Execution thread is done");
                }

                private void executeRunnables() {
                    Runnable runnable;
                    while ((runnable = queue.poll()) != null) {
                        try {
                            runnable.run();
                        } catch (final Exception e) {
                            LOGGER.log(AbstractLogger.ERROR, e.getMessage(), e);
                        }
                    }
                }

                private void doRequests(final QuikConnect quikConnect) {
                    try {
                        LOGGER.info("Correct requests:");
                        request(() -> quikConnect.responseMN("message(\"Hello, QLua-world!\", 2)", 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseCB("return os.sysdate()", 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("os.sysdate", null, 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseCB("os.sysdate", List.of(), 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("isConnected", null, 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("math.max", List.of(1, 3, 5, 7), 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("message", List.of("Hi, there!", 1), 5, TimeUnit.SECONDS));

                        LOGGER.info("Erroneous requests:");
                        request(() -> quikConnect.responseMN("return string(((", 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("return math.max(\"ABC\", 15)", 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("mess", null, 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseMN("math.max", List.of("ABC", 15), 5, TimeUnit.SECONDS));

                        LOGGER.info("Correct requests:");
                        request(() -> quikConnect.responseMN("return initDataSource(\"TQBR\", \"AFLT\", 1)", 5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseCB("OnAllTrade",
                                "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                                5, TimeUnit.SECONDS));
                        request(() -> quikConnect.responseCB("OnCandle",
                                "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                                5, TimeUnit.SECONDS));
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (final Exception e) {
                        LOGGER.log(AbstractLogger.ERROR, e.getMessage(), e);
                    }
                }

                private static void request(final Callable<JSONObject> callable) throws Exception {
                    long t = System.nanoTime();
                    final JSONObject json = callable.call();
                    t = (System.nanoTime() - t) / 1_000_000L;
                    LOGGER.info(t + " ms; " + json);
                }
            };
        }

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
    }
}
