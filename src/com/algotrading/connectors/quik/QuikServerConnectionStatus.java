package com.algotrading.connectors.quik;

import com.algotrading.base.util.AbstractLogger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.algotrading.connectors.quik.QuikDecoder.result;
import static com.algotrading.connectors.quik.QuikDecoder.status;

/**
 * Отслеживание наличия подключения терминала QUIK к серверу QUIK.
 */
public class QuikServerConnectionStatus implements QuikListener {
    /**
     * Логгер.
     */
    private final AbstractLogger logger;
    /**
     * Префикс для лога.
     */
    private final String prefix;
    /**
     * Подключение к терминалу QUIK.
     */
    private QuikConnect quikConnect = null;
    /**
     * Таймаут при выполнении запросов и получении ответов от терминала.
     */
    public long responseTimeoutMillis = 5000L;
    /**
     * Периодичность проверки наличия подключения терминала QUIK к серверу QUIK.
     */
    public long checkConnectedTimeoutMillis = 5000L;
    /**
     * Периодичность попыток подписки на коллбэк OnDisconnected.
     */
    public long failedSubscriptionTimeoutMillis = 5000L;

    private volatile boolean isRunning = true;
    private final Queue<Runnable> queue = new LinkedBlockingDeque<>();
    private ZonedDateTime connectedSince = null;

    /**
     * Класс для хранения логического значения и момента времени,
     * после которого требуется пересчёт логического значения.
     * Если в качестве момента времени используется {@code null},
     * пересчёт логического значения не нужен.
     */
    private record BooleanRetryTime(boolean b, ZonedDateTime retryTime) {
    }

    private static final BooleanRetryTime FALSE_NO_RETRY = new BooleanRetryTime(false, null);
    private static final BooleanRetryTime TRUE_NO_RETRY = new BooleanRetryTime(true, null);

    private CompletableFuture<BooleanRetryTime> cfIsSubscribed = CompletableFuture.completedFuture(FALSE_NO_RETRY);
    private CompletableFuture<BooleanRetryTime> cfIsConnected = CompletableFuture.completedFuture(FALSE_NO_RETRY);

    public QuikServerConnectionStatus(final AbstractLogger logger, final String clientId) {
        this.logger = logger;
        prefix = clientId + ": " + QuikServerConnectionStatus.class.getSimpleName() + ": ";
    }

    @Override
    public void setQuikConnect(final QuikConnect quikConnect) {
        this.quikConnect = quikConnect;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * @return момент времени, начиная с которого имеется подключение терминала QUIK к серверу QUIK;
     * {@code null}, если подключение отсутствует
     */
    public ZonedDateTime getConnectedSince() {
        return connectedSince;
    }

    private void executeAtNextStep(final Runnable runnable) {
        queue.add(runnable);
    }

    private void executeRunnables() {
        Runnable runnable;
        while ((runnable = queue.poll()) != null) {
            runnable.run();
        }
    }

    private void on() {
        final ZonedDateTime now = ZonedDateTime.now();
        cfIsSubscribed = CompletableFuture.completedFuture(new BooleanRetryTime(false, now));
        cfIsConnected = CompletableFuture.completedFuture(new BooleanRetryTime(false, now));
    }

    private void off() {
        connectedSince = null;
        cfIsSubscribed = CompletableFuture.completedFuture(FALSE_NO_RETRY);
        cfIsConnected = CompletableFuture.completedFuture(FALSE_NO_RETRY);
    }

    @Override
    public void onOpen() {
        if (!isRunning()) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "onOpen");
        }
        executeAtNextStep(this::on);
    }

    @Override
    public void onClose() {
        if (!isRunning()) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "onClose");
        }
        executeAtNextStep(this::off);
    }

    @Override
    public void onCallback(final JSONObject jsonObject) {
        if (!isRunning()) {
            return;
        }
        if ("OnDisconnected".equals(jsonObject.get("callback"))) {
            if (logger != null) {
                logger.info(prefix + "OnDisconnected");
            }
            executeAtNextStep(() -> {
                connectedSince = null;
                cfIsConnected = CompletableFuture.completedFuture(
                        new BooleanRetryTime(
                                false,
                                ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS)
                        )
                );
            });
        }
    }

    @Override
    public void onExceptionMN(final Exception exception) {
        if (!isRunning()) {
            return;
        }
        if (logger != null) {
            logger.warn(prefix + "onExceptionMN");
        }
        executeAtNextStep(this::off);
    }

    @Override
    public void onExceptionCB(final Exception exception) {
        if (!isRunning()) {
            return;
        }
        if (logger != null) {
            logger.warn(prefix + "onExceptionCB");
        }
        executeAtNextStep(this::off);
    }

    @Override
    public void step(final boolean isInterrupted) {
        if (!isRunning()) {
            return;
        }
        if (isInterrupted) {
            isRunning = false;
        }
        executeRunnables();
        ensureSubscription();
        checkConnected();
        if (isInterrupted && cfIsSubscribed.isDone() && cfIsConnected.isDone()) {
            isRunning = false;
        }
    }

    private void ensureSubscription() {
        if (!cfIsSubscribed.isDone()) {
            return;
        }
        final BooleanRetryTime booleanRetryTime = cfIsSubscribed.getNow(null);
        if (booleanRetryTime == null
                || booleanRetryTime.b()
                || booleanRetryTime.retryTime() == null
                || booleanRetryTime.retryTime().isAfter(ZonedDateTime.now())) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "Subscribe to OnDisconnect");
        }
        cfIsSubscribed = quikConnect.getResponseCB(
                        "OnDisconnected",
                        "*",
                        responseTimeoutMillis, TimeUnit.MILLISECONDS)
                .thenApply(response -> {
                    final boolean status = status(response);
                    logger.info(prefix + "subscribed OnDisconnected: " + status);
                    return status ?
                            TRUE_NO_RETRY :
                            new BooleanRetryTime(
                                    false,
                                    ZonedDateTime.now().plus(failedSubscriptionTimeoutMillis, ChronoUnit.MILLIS)
                            );
                }).exceptionally(t -> {
                    logger.log(AbstractLogger.ERROR, prefix + "Cannot subscribe to OnDisconnected", t);
                    return new BooleanRetryTime(
                            false,
                            ZonedDateTime.now().plus(failedSubscriptionTimeoutMillis, ChronoUnit.MILLIS)
                    );
                });
    }

    private void checkConnected() {
        if (!cfIsConnected.isDone()) {
            return;
        }
        final BooleanRetryTime booleanRetryTime = cfIsConnected.getNow(null);
        if (booleanRetryTime == null
                || booleanRetryTime.retryTime() == null
                || booleanRetryTime.retryTime().isAfter(ZonedDateTime.now())) {
            return;
        }
        if (logger != null) {
            logger.debug(prefix + "Check connected");
        }
        cfIsConnected = quikConnect.getResponseMN(
                "isConnected", null,
                responseTimeoutMillis, TimeUnit.MILLISECONDS
        ).thenApply(response -> {
            final boolean b = ((long) result(response) == 1L);
            if (logger != null) {
                logger.debug(prefix + "connected: " + b);
            }
            if (b && connectedSince == null) {
                connectedSince = ZonedDateTime.now();
            }
            if (!b) {
                connectedSince = null;
            }
            return new BooleanRetryTime(
                    b,
                    ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS)
            );
        }).exceptionally(t -> {
            if (logger != null) {
                logger.log(AbstractLogger.ERROR, prefix + ": Cannot check isConnected() == 1", t);
            }
            connectedSince = null;
            return new BooleanRetryTime(
                    false,
                    ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS)
            );
        });
    }
}