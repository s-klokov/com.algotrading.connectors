package com.algotrading.connectors.quik.deprecated;

import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.logs.SimpleLogger;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

class QuikAgentTest extends QuikAgent {

    private static final AbstractLogger LOGGER = new SimpleLogger();
    private boolean isRunning = false;
    private long stopTime = 0L;
    private ZonedDateTime dateTime = ZonedDateTime.now().withNano(0);

    public static void main(final String[] args) {
        final QuikAgentTest quikAgentTest = new QuikAgentTest(
                "localhost", 10001, 10002, QuikAgentTest.class.getSimpleName());
        quikAgentTest.init();
        quikAgentTest.run();
        quikAgentTest.shutdown();
    }

    QuikAgentTest(final String host, final int portMN, final int portCB, final String clientId) {
        super(host, portMN, portCB, clientId);
    }

    @Override
    public void init() {
        LOGGER.info("STARTED");
        super.init();
        isRunning = true;
        stopTime = System.currentTimeMillis() + 2 * 60 * 1000;
    }

    @Override
    public void shutdown() {
        isRunning = false;
        super.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    @Override
    public void onOpen() {
        super.onOpen();
        try {
            sendMN("message(\"Hello, QLua-world!\", 2)");
            sendMN("return os.sysdate()");
            sendMN("os.sysdate", List.of());
            sendMN("os.sysdate", null);
            sendMN("isConnected", null);
            sendMN("math.max", List.of(1, 3, 5, 7));
            sendMN("message", List.of("Hi, there!", 1));

            // Test responses for erroneous requests
            sendMN("return string(((");
            sendMN("return math.max(\"ABC\", 15)");
            sendMN("mess", null);
            sendMN("math.max", List.of("ABC", 15));
            // ---

            sendMN("return initDataSource(\"TQBR\", \"AFLT\", 1)");
            sendCB("OnAllTrade",
                    "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end");
            sendCB("OnCandle", "*");
        } catch (final IOException e) {
            onIOExceptionMN(e);
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning || super.isRunning();
    }

    @Override
    public void step(final boolean isInterrupted, final boolean hasErrorMN, final boolean hasErrorCB) {
        super.step(isInterrupted, hasErrorMN, hasErrorCB);
        if (!isRunning) {
            return;
        }
        final ZonedDateTime currDateTime = ZonedDateTime.now().withNano(0);
        boolean isNewSecond = false;
        if (currDateTime.isAfter(dateTime)) {
            dateTime = currDateTime;
            isNewSecond = true;
        }
        if (isInterrupted || System.currentTimeMillis() >= stopTime) {
            this.isInterrupted = true;
            isRunning = false;
            return;
        }
        if (hasErrorMN || hasErrorCB) {
            return;
        }
        if (isNewSecond && dateTime.getSecond() % 5 == 0) {
            try {
                sendMN("return getCandles(\"TQBR\", \"AFLT\", 1, 5)");
            } catch (final IOException e) {
                onIOExceptionMN(e);
            }
        }
    }

    @Override
    public void onReceiveMN(final JSONObject jsonObject) {
        super.onReceiveMN(jsonObject);
        LOGGER.info("MN: " + jsonObject.toJSONString());
    }

    @Override
    public void onReceiveCB(final JSONObject jsonObject) {
        super.onReceiveMN(jsonObject);
        LOGGER.info("CB: " + jsonObject.toJSONString());
    }

    @Override
    public void onIOExceptionMN(final IOException e) {
        super.onIOExceptionMN(e);
        LOGGER.error(e.toString());
    }

    @Override
    public void onIOExceptionCB(final IOException e) {
        super.onIOExceptionCB(e);
        LOGGER.error(e.toString());
    }

    @Override
    public void onException(final Exception e) {
        LOGGER.error(e.toString());
    }
}
