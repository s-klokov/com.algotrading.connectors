package com.algotrading.connectors.quik.deprecated;

import com.algotrading.base.util.TimeConditionTrigger;
import com.algotrading.connectors.quik.QuikCalendar;
import com.simpleutils.logs.AbstractLogger;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.algotrading.base.util.JSONConfig.getOrDefault;
import static com.algotrading.connectors.quik.QuikDecoder.result;
import static com.algotrading.connectors.quik.QuikDecoder.status;

/**
 * Реализация ожидания подключения терминала QUIK к серверу QUIK.
 * <p>
 * Если терминал QUIK подключается к серверу QUIK, то ждём время {@link #reconnectedTimeout},
 * пока терминал прокачает данные и станет готовым к работе.
 * Если после события onOpen терминал QUIK сразу возвращает isConnected() == 1,
 * то ожидаем время {@link #openConnectedTimeout}.
 * <p>
 * В случае готовности метод {@link #isReady()} возвращает {@code true}.
 * <p>
 * Если терминал отключается, то метод {@link #isReady()} через некоторое время станет
 * возвращать {@code false}.
 * <p>
 * Запросы к терминалу, проверяющие условие isConnected() == 1, делаются с периодичностью
 * {@link #checkConnectedTimeout}.
 */
public class QuikReconnectProtection implements QuikInterface {

    /**
     * Объект типа {@link QuikInterface}, используемый для отправки и приёма сообщений.
     */
    private final QuikInterface quikInterface;
    /**
     * Календарь, задающий режим работы.
     */
    private final QuikCalendar quikCalendar;
    /**
     * Логгер.
     */
    private final AbstractLogger logger;
    /**
     * Префикс для лога.
     */
    private final String prefix;
    /**
     * Время ожидания в миллисекундах, если терминал сразу подключен к серверу.
     */
    public long openConnectedTimeout = 15_000L;
    /**
     * Время ожидания в миллисекундах, если терминал подключился к серверу.
     */
    public long reconnectedTimeout = 60_000L;
    /**
     * Периодичность проверки условия isConnected() == 1, заданный в миллисекундах.
     */
    public long checkConnectedTimeout = 5000L;
    /**
     * Таймаут при запросах к терминалу, заданный в миллисекундах.
     */
    public long requestTimeout = 5000L;
    /**
     * Готов ли терминал к работе.
     */
    private boolean isReady = false;

    private boolean isInterrupted = true;
    private boolean isRunning = false;

    private Boolean wasConnected = null;
    private CompletableFuture<Boolean> isConnected = null;
    private TimeConditionTrigger checkTrigger = null;
    private TimeConditionTrigger reconnectTrigger = null;
    private CompletableFuture<Boolean> isSubscribed = null;

    public QuikReconnectProtection(final QuikInterface quikInterface,
                                   final QuikCalendar quikCalendar,
                                   final AbstractLogger logger,
                                   final String clientId) {
        this.quikInterface = quikInterface;
        this.quikCalendar = quikCalendar;
        this.logger = logger;
        prefix = clientId + ": Reconnect protection: ";
    }

    public boolean isReady() {
        return isReady;
    }

    @Override
    public void configurate(final JSONAware config) {
        final JSONObject json = (JSONObject) config;
        openConnectedTimeout = getOrDefault(json, "openConnectedTimeout", openConnectedTimeout);
        reconnectedTimeout = getOrDefault(json, "reconnectedTimeout", reconnectedTimeout);
        checkConnectedTimeout = getOrDefault(json, "checkConnectedTimeout", checkConnectedTimeout);
        requestTimeout = getOrDefault(json, "requestTimeout", requestTimeout);
    }

    @Override
    public void init() {
        isInterrupted = false;
        isRunning = true;
        isReady = false;
        wasConnected = null;
        isConnected = null;
        checkTrigger = null;
        reconnectTrigger = null;
        isSubscribed = null;
    }

    @Override
    public void shutdown() {
        isSubscribed = null;
        reconnectTrigger = null;
        checkTrigger = null;
        isConnected = null;
        wasConnected = null;
        isReady = false;
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
            if ((isConnected == null || isConnected.isDone())
                    && (isSubscribed == null || isSubscribed.isDone())) {
                isRunning = false;
                return;
            }
        }

        if (hasErrorMN || hasErrorCB || (quikCalendar != null && !quikCalendar.isWorking())) {
            isReady = false;
            reconnectTrigger = null;
            isConnected = null;
            isSubscribed = null;
            return;
        }

        if (isSubscribed == null) {
            logger.info(prefix + "subscribe to OnDisconnected");
            isSubscribed = getResponseCB("OnDisconnected", "*",
                    requestTimeout, TimeUnit.MILLISECONDS
            ).thenApply(response -> {
                final boolean status = status(response);
                logger.info(prefix + "subscribed OnDisconnected: " + status);
                return status;
            }).exceptionally(t -> {
                logger.log(AbstractLogger.ERROR, prefix + "Cannot subscribe to OnDisconnected", t);
                return false;
            });
        }

        if (checkTrigger != null && checkTrigger.triggered()) {
            wasConnected = (isConnected == null) ? null : isConnected.getNow(false);
            isConnected = getResponseMN(
                    "isConnected", null,
                    requestTimeout, TimeUnit.MILLISECONDS
            ).thenApply(response -> {
                final boolean b = ((long) result(response) == 1);
                logger.info(prefix + "connected: " + b);
                if (wasConnected == null) {
                    if (b) {
                        reconnectTrigger = TimeConditionTrigger.newDelayTrigger(openConnectedTimeout, ChronoUnit.MILLIS);
                    }
                } else if (!wasConnected) {
                    if (b) {
                        reconnectTrigger = TimeConditionTrigger.newDelayTrigger(reconnectedTimeout, ChronoUnit.MILLIS);
                    }
                }
                if (!b) {
                    isReady = false;
                    reconnectTrigger = null;
                }
                return b;
            }).exceptionally(t -> {
                logger.log(AbstractLogger.ERROR, prefix + ": Error", t);
                isReady = false;
                reconnectTrigger = null;
                return false;
            });
        }

        if (reconnectTrigger != null && reconnectTrigger.triggered()) {
            reconnectTrigger = null;
            logger.info(prefix + "ready");
            isReady = true;
        }
    }

    @Override
    public void onOpen() {
        isReady = false;
        wasConnected = null;
        isConnected = null;
        checkTrigger = TimeConditionTrigger.newPeriodicTrigger(checkConnectedTimeout, ChronoUnit.MILLIS);
        reconnectTrigger = null;
        isSubscribed = null;
        logger.info(prefix + "onOpen");
        if (quikCalendar != null) {
            logger.info(prefix + "isWorking: " + quikCalendar.isWorking());
        }
    }

    @Override
    public void onClose() {
        isReady = false;
        wasConnected = null;
        isConnected = null;
        checkTrigger = null;
        reconnectTrigger = null;
        isSubscribed = null;
        logger.info(prefix + "onClose");
    }

    @Override
    public void onReceiveMN(final JSONObject jsonObject) {
    }

    @Override
    public void onReceiveCB(final JSONObject jsonObject) {
        if ("OnDisconnected".equals(jsonObject.get("callback"))) {
            isReady = false;
            reconnectTrigger = null;
            logger.info(prefix + "OnDisconnected");
        }
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
}
