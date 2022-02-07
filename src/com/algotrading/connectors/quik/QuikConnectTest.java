package com.algotrading.connectors.quik;

import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.SimpleLogger;
import org.json.simple.JSONObject;

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
        try {
            Thread.sleep(5000L);
        } catch (final InterruptedException ignored) {
        }
        LOGGER.info("Shutting down QuikConnect");
        quikConnect.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    static class TestListener implements QuikListener {

        @Override
        public void onOpen() {
            LOGGER.info("OnOpen");
        }

        @Override
        public void onClose() {
            LOGGER.info("OnClose");
        }

        @Override
        public void onCallback(final JSONObject jsonObject) {
            LOGGER.info("OnCallBack");
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
