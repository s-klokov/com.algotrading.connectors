package com.algotrading.connectors.quik.deprecated;

import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.SimpleLogger;
import com.algotrading.base.util.TimeConditionTrigger;
import com.algotrading.connectors.quik.QuikDecoder;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class QuikAgentTest2 extends QuikAgent {

    private static final AbstractLogger LOGGER = new SimpleLogger();
    private boolean isRunning = false;

    private QuikReconnectProtection quikReconnectProtection = null;
    private QuikCandlesStorage quikCandlesStorage = null;

    private TimeConditionTrigger stopTrigger = null;
    private TimeConditionTrigger getCandlesTrigger = null;
    private TimeConditionTrigger updateQuikCandlesStorageTrigger = null;
    private static final CompletableFuture<JSONObject> START = CompletableFuture.completedFuture(new JSONObject());
    private CompletableFuture<JSONObject> openChain = null;
    private List<CompletableFuture<JSONObject>> openBatch = null;
    private CompletableFuture<JSONObject> initDataSources = null;
    private boolean isOpenComplete = false;
    private boolean dataSourceSizesComplete = false;
    private final Map<String, CompletableFuture<Integer>> updateCandlesResults = new HashMap<>();

    public static void main(final String[] args) {
        final QuikAgentTest2 quikAgentTest = new QuikAgentTest2(
                "localhost", 10001, 10002, QuikAgentTest2.class.getSimpleName());
        quikAgentTest.init();
        quikAgentTest.run();
        quikAgentTest.shutdown();
    }

    QuikAgentTest2(final String host, final int portMN, final int portCB, final String clientId) {
        super(host, portMN, portCB, clientId);
    }

    @Override
    public void init() {
        LOGGER.info("STARTED");
        super.init();
        quikReconnectProtection = new QuikReconnectProtection(this, null, LOGGER, clientId);
        quikReconnectProtection.init();
        quikCandlesStorage = new QuikCandlesStorage(this);
        final String classCode = "TQBR";
        for (final String secCode : List.of("SBER", "GAZP", "AFLT", "MAGN")) {
            for (final int period : List.of(1, 5, 15)) {
                quikCandlesStorage.add(new QuikDataSourceCandles(
                        classCode, secCode, period, TimeUnit.MINUTES,
                        new int[]{3, 30, 300, 3000}, 6000, 5000, false));
            }
        }
        quikCandlesStorage.init();
        stopTrigger = TimeConditionTrigger.newDelayTrigger(10, ChronoUnit.MINUTES);
        getCandlesTrigger = new TimeConditionTrigger(now -> now.getSecond() % 5 == 0);
        updateQuikCandlesStorageTrigger = new TimeConditionTrigger(now -> now.getSecond() % 20 == 3);
        isRunning = true;
    }

    @Override
    public void shutdown() {
        isRunning = false;
        quikCandlesStorage.shutdown();
        quikReconnectProtection.shutdown();
        super.shutdown();
        LOGGER.info("SHUTDOWN");
    }

    @Override
    public void onOpen() {
        super.onOpen();
        quikReconnectProtection.onOpen();
        quikCandlesStorage.onOpen();
        openChain = null;
        openBatch = null;
        initDataSources = null;
        isOpenComplete = false;
        dataSourceSizesComplete = false;
    }

    @Override
    public void onClose() {
        super.onClose();
        quikReconnectProtection.onClose();
        quikCandlesStorage.onClose();
    }

    @Override
    public boolean isRunning() {
        return isRunning || isMemberRunning() || super.isRunning();
    }

    private boolean isMemberRunning() {
        return quikReconnectProtection.isRunning() || quikCandlesStorage.isRunning();
    }

    private boolean isOpenBatchDone() {
        for (final CompletableFuture<JSONObject> response : openBatch) {
            if (!response.isDone()) {
                return false;
            }
        }
        return true;
    }

    private void ensureOpenChain() {
        if (openChain == null) {
            openChain = START
                    .thenCompose(response -> {
                        LOGGER.info("Hello request");
                        return getResponseMN(
                                "message", List.of("Hello, QLua-world!", 2),
                                5, TimeUnit.SECONDS);
                    })
                    .thenCompose(response -> {
                        LOGGER.info("Hello response");
                        if (!QuikDecoder.status(response)) {
                            return CompletableFuture.failedFuture(new IllegalArgumentException());
                        }
                        LOGGER.info("SysDate request");
                        return getResponseMN(
                                "os.sysdate", null,
                                5, TimeUnit.SECONDS);
                    })
                    .thenCompose(response -> {
                        LOGGER.info("SysDate response");
                        if (!QuikDecoder.status(response)) {
                            return CompletableFuture.failedFuture(new IllegalArgumentException());
                        }
                        LOGGER.info("InitDataSource for AFLT");
                        return getResponseMN(
                                "initDataSource", List.of("TQBR", "AFLT", 1),
                                5, TimeUnit.SECONDS);
                    })
                    .thenCompose(response -> {
                        if (!QuikDecoder.status(response)) {
                            return CompletableFuture.failedFuture(new IllegalArgumentException());
                        }
                        LOGGER.info("Callback subscription");
                        return getResponseCB(
                                "OnAllTrade",
                                "function(t) return t.class_code == \"TQBR\" and t.sec_code == \"AFLT\" end",
                                5, TimeUnit.SECONDS);
                    });
        }
    }

    private void ensureOpenBatch() {
        if (openBatch == null) {
            openBatch = List.of(
                    getResponseMN("initDataSource", List.of("TQBR", "SBER", 1),
                                  5, TimeUnit.SECONDS),
                    getResponseMN("initDataSource", List.of("TQBR", "SBER", 5),
                                  5, TimeUnit.SECONDS),
                    getResponseMN("initDataSource", List.of("TQBR", "SBER", 15),
                                  5, TimeUnit.SECONDS)
            );
        }
    }

    private void ensureInitDataSources() {
        if (initDataSources == null) {
            initDataSources = quikCandlesStorage.initDataSources(1, TimeUnit.SECONDS);
        }
    }

    @Override
    public void step(final boolean isInterrupted, final boolean hasErrorMN, final boolean hasErrorCB) {
        super.step(isInterrupted, hasErrorMN, hasErrorCB);
        quikReconnectProtection.step(isInterrupted, hasErrorMN, hasErrorCB);
        quikCandlesStorage.step(isInterrupted, hasErrorMN, hasErrorCB);

        if (isInterrupted || stopTrigger.triggered()) {
            this.isInterrupted = true;
        }
        if (!isRunning) {
            return;
        }
        if (this.isInterrupted && !isMemberRunning()) {
            isRunning = false;
            return;
        }

        if (hasErrorMN || hasErrorCB) {
            return;
        }

        if (!quikReconnectProtection.isReady()) {
            return;
        }
        ensureOpenChain();
        ensureOpenBatch();
        ensureInitDataSources();
        if (!isOpenComplete && openChain != null) {
            LOGGER.info("Open chain done: " + openChain.isDone());
        }
        if (!isOpenComplete && openBatch != null) {
            LOGGER.info("Open batch done: " + isOpenBatchDone());
        }
        if (!isOpenComplete && initDataSources != null) {
            LOGGER.info("InitDataSources done: " + initDataSources.isDone());
        }

        if (openChain != null && openChain.isDone()
            && openBatch != null && isOpenBatchDone()
            && initDataSources != null && initDataSources.isDone()) {
            isOpenComplete = true;
        }
        if (!isOpenComplete) {
            if (!initDataSources.isDone()) {
                LOGGER.info("initDataSources: in process");
            } else if (initDataSources.isCompletedExceptionally()) {
                LOGGER.info("initDataSources: exception");
            } else {
                LOGGER.info("initDataSources: done");
                final JSONObject json = initDataSources.getNow(null);
                LOGGER.info("Result: " + json.toJSONString());
                dataSourceSizesComplete = false;
                quikCandlesStorage.getDataSourceSizes(1, TimeUnit.SECONDS);
            }
        }
        if (!dataSourceSizesComplete) {
            final CompletableFuture<JSONObject> future = quikCandlesStorage.lastDataSourceSizesResult;
            if (future == null) {
                LOGGER.info("dataSourceSizes: null");
            } else if (!future.isDone()) {
                LOGGER.info("dataSourceSizes: in process");
            } else if (future.isCompletedExceptionally()) {
                LOGGER.info("dataSourceSizes: exception");
                dataSourceSizesComplete = true;
            } else {
                LOGGER.info("dataSourceSizes: done");
                final JSONObject json = future.getNow(null);
                LOGGER.info("Result: " + json.toJSONString());
                dataSourceSizesComplete = true;
            }
        }
        if (getCandlesTrigger.triggered()) {
            try {
                sendMN("getCandles", List.of("TQBR", "AFLT", 1, 5));
            } catch (final IOException e) {
                onIOExceptionMN(e);
            }
            if (dataSourceSizesComplete) {
                dataSourceSizesComplete = false;
                quikCandlesStorage.getDataSourceSizes(1, TimeUnit.SECONDS);
            }
        }
        if (updateQuikCandlesStorageTrigger.triggered()) {
            updateCandlesResults.clear();
            quikCandlesStorage.dataSourcesMap.forEach(
                    (key, value) -> {
                        LOGGER.info("Update quik candles: " + key);
                        updateCandlesResults.put(
                                key,
                                quikCandlesStorage.updateDataSourceCandles(value, 5, TimeUnit.SECONDS)
                        );
                    }
            );
        }
        updateCandlesResults.forEach(
                (key, value) -> {
                    if (value.isDone()) {
                        if (value.isCompletedExceptionally()) {
                            LOGGER.error(key);
                        } else {
                            LOGGER.info(key + " update result: " + value.getNow(-100));
                        }
                    }
                }
        );
        updateCandlesResults.entrySet().removeIf(e -> e.getValue().isDone());
    }

    @Override
    public void onReceiveMN(final JSONObject jsonObject) {
        super.onReceiveMN(jsonObject);
        quikReconnectProtection.onReceiveMN(jsonObject);
        quikCandlesStorage.onReceiveMN(jsonObject);
        String jsonString = jsonObject.toJSONString();
        if (jsonString.length() > 500) {
            jsonString = jsonString.substring(0, 500) + "...";
        }
        LOGGER.info("MN: " + jsonString);
    }

    @Override
    public void onReceiveCB(final JSONObject jsonObject) {
        super.onReceiveCB(jsonObject);
        quikReconnectProtection.onReceiveCB(jsonObject);
        quikCandlesStorage.onReceiveCB(jsonObject);
        LOGGER.info("CB: " + jsonObject.toJSONString());
    }

    @Override
    public void onIOExceptionMN(final IOException e) {
        super.onIOExceptionMN(e);
        quikReconnectProtection.onIOExceptionMN(e);
        quikCandlesStorage.onIOExceptionMN(e);
        LOGGER.error(e.toString());
    }

    @Override
    public void onIOExceptionCB(final IOException e) {
        super.onIOExceptionCB(e);
        quikReconnectProtection.onIOExceptionCB(e);
        quikCandlesStorage.onIOExceptionCB(e);
        LOGGER.error(e.toString());
    }

    @Override
    public void onException(final Exception e) {
        quikReconnectProtection.onException(e);
        quikCandlesStorage.onException(e);
        LOGGER.error(e.toString());
    }
}
