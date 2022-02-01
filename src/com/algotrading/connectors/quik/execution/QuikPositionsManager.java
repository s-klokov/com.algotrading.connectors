package com.algotrading.connectors.quik.execution;

import com.algotrading.base.helpers.IOHelper;
import com.algotrading.base.util.AbstractLogger;
import com.algotrading.base.util.TimeConditionTrigger;
import com.algotrading.connectors.quik.QuikDecoder;
import com.algotrading.connectors.quik.QuikInterface;
import com.algotrading.connectors.quik.QuikReconnectProtection;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.algotrading.base.util.JSONConfig.getOrDefault;
import static com.algotrading.base.util.JSONConfig.getStringNonNull;

public class QuikPositionsManager implements QuikInterface {

    /**
     * Объект типа {@link QuikInterface}, используемый для отправки и приёма сообщений.
     */
    private final QuikInterface quikInterface;
    /**
     * Объект типа {@link QuikReconnectProtection}.
     */
    private final QuikReconnectProtection quikReconnectProtection;
    /**
     * Логгер.
     */
    private final AbstractLogger logger;
    /**
     * Префикс для лога.
     */
    private final String prefix;
    /**
     * Таймаут при запросах к терминалу, заданный в миллисекундах.
     */
    public long requestTimeout = 5000L;
    /**
     * Таймаут перед попыткой повторной подписки в случае ошибки.
     */
    public long retryTimeout = 15_000L;

    public long serviceTimeout = 15_000L;
    public long unknownTradeReplyTimeout = 3600_000L;
    public long limitReplyTimeout = 600_000L;
    public long killReplyTimeout = 180_000L;
    public long missingTradesTimeout = 30_000L;
    public long missingOrdersTimeout = 30_000L;
    /**
     * Минимальный номер транзакции (включая).
     */
    public long transIdMin = 501_000_000L;
    /**
     * Максимальный номер транзакции (исключая).
     */
    public long transIdMax = 502_000_000L;
    /**
     * Скачок номера транзакции для обеспечения уникальности.
     */
    public long transIdJump = 10_000L;
    /**
     * Периодичность записи текущего номера транзакции в файл.
     */
    public long transIdTimeout = 60_000L;
    /**
     * Имя файла с номером транзакции.
     */
    public String transIdFileName = null;
    /**
     * Множество "своих" значений brokerRef.
     */
    public final Set<String> brokerRefSet = new HashSet<>();

    private boolean isInterrupted = true;
    private boolean isRunning = false;

    /**
     * Будущий результат подписки на коллбэки OnTransReply, OnOrder, OnTrade.
     */
    private CompletableFuture<Boolean> isSubscribed = null;
    /**
     * Триггер для повторения подписки в случае ошибки.
     */
    private TimeConditionTrigger retrySubscriptionTrigger = null;
    /**
     * Результат отправки последней трензакции.
     */
    private CompletableFuture<JSONObject> lastSendTransactionResult = null;
    /**
     * Последнее выданное значение номера транзакции.
     */
    private long transId = transIdMin;
    /**
     * Триггер для записи значения transId в файл.
     */
    private TimeConditionTrigger transIdTrigger = null;

    public QuikPositionsManager(final QuikInterface quikInterface,
                                final QuikReconnectProtection quikReconnectProtection,
                                final AbstractLogger logger,
                                final String clientId) {
        this.quikInterface = quikInterface;
        this.quikReconnectProtection = quikReconnectProtection;
        this.logger = logger;
        prefix = clientId + ": PositionsManager: ";
    }

    @Override
    public void configurate(final JSONAware config) {
        final JSONObject json = (JSONObject) config;
        requestTimeout = getOrDefault(json, "requestTimeout", requestTimeout);
        retryTimeout = getOrDefault(json, "retryTimeout", retryTimeout);

        serviceTimeout = getOrDefault(json, "serviceTimeout", serviceTimeout);
        unknownTradeReplyTimeout = getOrDefault(json, "unknownTradeReplyTimeout", unknownTradeReplyTimeout);
        limitReplyTimeout = getOrDefault(json, "limitReplyTimeout", limitReplyTimeout);
        killReplyTimeout = getOrDefault(json, "killReplyTimeout", killReplyTimeout);
        missingTradesTimeout = getOrDefault(json, "missingTradesTimeout", missingTradesTimeout);
        missingOrdersTimeout = getOrDefault(json, "missingOrdersTimeout", missingOrdersTimeout);

        transIdMin = getOrDefault(json, "transIdMin", transIdMin);
        transIdMax = getOrDefault(json, "transIdMax", transIdMax);
        transIdJump = getOrDefault(json, "transIdJump", transIdJump);
        transIdTimeout = getOrDefault(json, "transIdTimeout", transIdTimeout);
        transIdFileName = getStringNonNull(json, "transIdFileName");
        try (final BufferedReader br = IOHelper.getBufferedReader(transIdFileName)) {
            transId = Long.parseLong(br.readLine());
            if (transId < transIdMin || transId >= transIdMax) {
                transId = transIdMin;
            }
        } catch (final IOException e) {
            logger.log(AbstractLogger.ERROR, prefix + "Cannot read transId from file " + transIdFileName, e);
            transId = transIdMin;
            throw new IllegalArgumentException("Reading config error");
        }
        transIdTrigger = TimeConditionTrigger.newPeriodicTrigger(transIdTimeout, ChronoUnit.MILLIS);

        brokerRefSet.clear();
        for (final Object o : (JSONArray) json.get("brokerRefs")) {
            brokerRefSet.add((String) o);
        }
    }

    private long getNextTransId() {
        transId++;
        if (transId < transIdMin || transId >= transIdMax) {
            transId = transIdMin;
        }
        if (transIdTrigger.triggered()) {
            try (final PrintStream ps = IOHelper.getPrintStream(transIdFileName)) {
                ps.println(transId);
            } catch (final IOException e) {
                logger.log(AbstractLogger.ERROR, prefix + "Cannot write transId to file " + transIdFileName, e);
            }
        }
        return transId;
    }

    @Override
    public void init() {
        isInterrupted = false;
        isRunning = true;
        isSubscribed = null;
        lastSendTransactionResult = null;
    }

    @Override
    public void shutdown() {
        lastSendTransactionResult = null;
        isSubscribed = null;
        isRunning = false;
        isInterrupted = true;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public boolean hasErrorMN() {
        return quikInterface.hasErrorMN();
    }

    @Override
    public boolean hasErrorCB() {
        return quikInterface.hasErrorCB();
    }

    @Override
    public void step(final boolean isInterrupted, final boolean hasErrorMN, final boolean hasErrorCB) {
        if (isInterrupted) {
            this.isInterrupted = true;
        }
        if (!isRunning) {
            return;
        }
        if (this.isInterrupted) {
            if (canStop()) {
                // TODO: сохранение состояния перед завершением работы
                isRunning = false;
                return;
            }
        }

        if (hasErrorMN || hasErrorCB || !quikReconnectProtection.isReady()) {
            isSubscribed = null;
            return;
        }

        if (isSubscribed == null) {
            logger.info(prefix + "subscribe to OnTransReply");
            isSubscribed = getResponseCB("OnTransReply", "*",
                    requestTimeout, TimeUnit.MILLISECONDS
            ).thenCompose(response -> {
                if (!QuikDecoder.status(response)) {
                    throw new IllegalStateException("Cannot subscribe to OnTransReply");
                }
                logger.info(prefix + "subscribe to OnOrder");
                return getResponseCB("OnOrder", "*",
                        requestTimeout, TimeUnit.MILLISECONDS);
            }).thenCompose(response -> {
                if (!QuikDecoder.status(response)) {
                    throw new IllegalStateException("Cannot subscribe to OnOrder");
                }
                logger.info(prefix + "subscribe to OnTrade");
                return getResponseCB("OnTrade", "*",
                        requestTimeout, TimeUnit.MILLISECONDS);
            }).thenApply(response -> {
                if (!QuikDecoder.status(response)) {
                    throw new IllegalStateException("Cannot subscribe to OnTrade");
                }
                logger.info(prefix + "subscription is done");
                return true;
            }).exceptionally(t -> {
                logger.log(AbstractLogger.ERROR, prefix + "Cannot subscribe to callbacks", t);
                return false;
            });
        }
        if (isSubscribed.isDone()
                && (isSubscribed.isCompletedExceptionally() || !isSubscribed.getNow(false))) {
            if (retrySubscriptionTrigger == null) {
                retrySubscriptionTrigger = TimeConditionTrigger.newDelayTrigger(retryTimeout, ChronoUnit.MILLIS);
            } else if (retrySubscriptionTrigger.triggered()) {
                retrySubscriptionTrigger = null;
                isSubscribed = null;
            }
        }
    }

    @Override
    public void onOpen() {
        isSubscribed = null;
        lastSendTransactionResult = null;
        retrySubscriptionTrigger = null;
        // TODO:
    }

    @Override
    public void onClose() {
        isSubscribed = null;
        lastSendTransactionResult = null;
        retrySubscriptionTrigger = null;
        // TODO:
    }

    @Override
    public void onReceiveMN(final JSONObject jsonObject) {
    }

    @Override
    public void onReceiveCB(final JSONObject jsonObject) {
        final String callback = (String) jsonObject.get("callback");
        if ("OnTransReply".equals(callback)) {
            onTransReply((JSONObject) jsonObject.get("arg1"));
        } else if ("OnOrder".equals(callback)) {
            onOrder((JSONObject) jsonObject.get("arg1"));
        } else if ("OnTrade".equals(callback)) {
            onTrade((JSONObject) jsonObject.get("arg1"));
        }
        // TODO: возможно, что нужны подписка и реакция на OnAllTrade.
    }

    @Override
    public void onIOExceptionMN(final IOException e) {
    }

    @Override
    public void onIOExceptionCB(final IOException e) {
    }

    @Override
    public void onException(final Exception e) {
    }

    @Override
    public void sendMN(final String chunk) throws IOException {
        quikInterface.sendMN(chunk);
    }

    @Override
    public void sendMN(final String fname, final List<?> args) throws IOException {
        quikInterface.sendMN(fname, args);
    }

    @Override
    public void sendCB(final String chunk) throws IOException {
        quikInterface.sendCB(chunk);
    }

    @Override
    public void sendCB(final String fname, final List<?> args) throws IOException {
        quikInterface.sendCB(fname, args);
    }

    @Override
    public void sendCB(final String callback, final String filter) throws IOException {
        quikInterface.sendCB(callback, filter);
    }

    @Override
    public CompletableFuture<JSONObject> getResponseMN(final String chunk, final long timeout, final TimeUnit unit) {
        return quikInterface.getResponseMN(chunk, timeout, unit);
    }

    @Override
    public CompletableFuture<JSONObject> getResponseMN(final String fname, final List<?> args, final long timeout, final TimeUnit unit) {
        return quikInterface.getResponseMN(fname, args, timeout, unit);
    }

    @Override
    public CompletableFuture<JSONObject> getResponseCB(final String chunk, final long timeout, final TimeUnit unit) {
        return quikInterface.getResponseCB(chunk, timeout, unit);
    }

    @Override
    public CompletableFuture<JSONObject> getResponseCB(final String fname, final List<?> args, final long timeout, final TimeUnit unit) {
        return quikInterface.getResponseCB(fname, args, timeout, unit);
    }

    @Override
    public CompletableFuture<JSONObject> getResponseCB(final String callback, final String filter, final long timeout, final TimeUnit unit) {
        return quikInterface.getResponseCB(callback, filter, timeout, unit);
    }

    /**
     * @return {@code true}, если в текущий момент можно завершить работу
     */
    private boolean canStop() {
        // TODO: проверить, что закончены все действия
        return (isSubscribed == null || isSubscribed.isDone())
                && (lastSendTransactionResult == null || lastSendTransactionResult.isDone());
    }

    private void onTransReply(final JSONObject jsonTransReply) {
        // TODO:
    }

    private void onOrder(final JSONObject jsonOrder) {
        // TODO:
    }

    private void onTrade(final JSONObject jsonTrade) {
        // TODO:
    }

    /**
     * Отправить транзакцию в терминале QUIK.
     *
     * @param transaction соответствие, задающее содержимое транзакции
     * @return будущий результат отправки транзакции
     */
    private CompletableFuture<JSONObject> sendTransaction(final Map<String, String> transaction) {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("return sendTransaction({ ");
        transaction.forEach((k, v) -> sb.append(k).append("=\"").append(v).append("\", "));
        sb.append("})");
        lastSendTransactionResult = getResponseMN(sb.toString(), requestTimeout, TimeUnit.MILLISECONDS);
        return lastSendTransactionResult;
    }
}
