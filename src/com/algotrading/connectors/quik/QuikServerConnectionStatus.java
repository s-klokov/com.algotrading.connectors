package com.algotrading.connectors.quik;

import com.algotrading.base.util.AbstractLogger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;

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
    private long responseTimeoutMillis = 5000L;
    /**
     * Периодичность проверки наличия подключения терминала QUIK к серверу QUIK.
     */
    private long checkConnectedTimeoutMillis = 5000L;
    /**
     * Периодичность попыток подписки на коллбэк OnDisconnected.
     */
    private long failedSubscriptionTimeoutMillis = 5000L;

    private volatile boolean isRunning = true;
    private final Queue<Runnable> queue = new LinkedBlockingDeque<>();
    private ZonedDateTime connectedSince = null;
    /**
     * Объект для реализации подписки на коллбэк OnDisconnected.
     */
    private CompletableFuture<BooleanRetryTime> cfIsSubscribed = CompletableFuture.completedFuture(BooleanRetryTime.FALSE_NO_RETRY);
    /**
     * Объект для реализации периодической проверки isConnected() == 1.
     */
    private CompletableFuture<BooleanRetryTime> cfIsConnected = CompletableFuture.completedFuture(BooleanRetryTime.FALSE_NO_RETRY);

    /**
     * Конструктор.
     *
     * @param logger   логгер
     * @param clientId идентификатор клиента
     */
    public QuikServerConnectionStatus(final AbstractLogger logger, final String clientId) {
        this.logger = logger;
        prefix = clientId + ": " + QuikServerConnectionStatus.class.getSimpleName() + ": ";
    }

    public QuikServerConnectionStatus withResponseTimeoutMillis(final long responseTimeoutMillis) {
        this.responseTimeoutMillis = responseTimeoutMillis;
        return this;
    }

    public QuikServerConnectionStatus withCheckConnectedTimeoutMillis(final long checkConnectedTimeoutMillis) {
        this.checkConnectedTimeoutMillis = checkConnectedTimeoutMillis;
        return this;
    }

    public QuikServerConnectionStatus withFailedSubscriptionTimeoutMillis(final long failedSubscriptionTimeoutMillis) {
        this.failedSubscriptionTimeoutMillis = failedSubscriptionTimeoutMillis;
        return this;
    }

    public long responseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    public long checkConnectedTimeoutMillis() {
        return checkConnectedTimeoutMillis;
    }

    public long failedSubscriptionTimeoutMillis() {
        return failedSubscriptionTimeoutMillis;
    }

    @Override
    public void setQuikConnect(final QuikConnect quikConnect) {
        this.quikConnect = Objects.requireNonNull(quikConnect);
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * @return момент времени, начиная с которого имеется подключение терминала QUIK к серверу QUIK;
     * {@code null}, если подключение отсутствует
     */
    public ZonedDateTime connectedSince() {
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
        cfIsSubscribed = CompletableFuture.completedFuture(new BooleanRetryTime(false, ZonedDateTime.now()));
    }

    private void off() {
        connectedSince = null;
        cfIsSubscribed = CompletableFuture.completedFuture(BooleanRetryTime.FALSE_NO_RETRY);
        cfIsConnected = CompletableFuture.completedFuture(BooleanRetryTime.FALSE_NO_RETRY);
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
        if (!"OnDisconnected".equals(jsonObject.get("callback"))) {
            return;
        }
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
        executeRunnables();
        ensureSubscription();
        checkConnected();
        if (isInterrupted && cfIsSubscribed.isDone() && cfIsConnected.isDone()) {
            isRunning = false;
            off();
        }
    }

    private static BooleanRetryTime getBooleanRetryTime(final CompletableFuture<BooleanRetryTime> cf) {
        try {
            return cf.getNow(null);
        } catch (final CancellationException | CompletionException e) {
            return null;
        }
    }

    private void ensureSubscription() {
        if (!cfIsSubscribed.isDone()) {
            return;
        }
        final BooleanRetryTime booleanRetryTime = getBooleanRetryTime(cfIsSubscribed);
        if (booleanRetryTime == null
                || booleanRetryTime.b()
                || booleanRetryTime.retryTime() == null
                || booleanRetryTime.retryTime().isAfter(ZonedDateTime.now())) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "Subscribe to OnDisconnect");
        }
        cfIsSubscribed = quikConnect.futureResponseCB(
                        "OnDisconnected",
                        "*",
                        responseTimeoutMillis, TimeUnit.MILLISECONDS)
                .thenApply(response -> {
                    final boolean status = status(response);
                    if (logger != null) {
                        logger.info(prefix + "subscribed OnDisconnected: " + status);
                    }
                    if (status) {
                        cfIsConnected = CompletableFuture.completedFuture(new BooleanRetryTime(false, ZonedDateTime.now()));
                        return BooleanRetryTime.TRUE_NO_RETRY;
                    } else {
                        if (logger != null) {
                            logger.error(prefix + "Cannot subscribe to OnDisconnected");
                        }
                        return new BooleanRetryTime(
                                false,
                                ZonedDateTime.now().plus(failedSubscriptionTimeoutMillis, ChronoUnit.MILLIS)
                        );
                    }
                }).exceptionally(t -> {
                    if (logger != null) {
                        logger.log(AbstractLogger.ERROR, prefix + "Cannot subscribe to OnDisconnected", t);
                    }
                    return new BooleanRetryTime(
                            false,
                            ZonedDateTime.now().plus(failedSubscriptionTimeoutMillis, ChronoUnit.MILLIS)
                    );
                });
    }

    private void checkConnected() {
        if (!cfIsSubscribed.isDone()) {
            return;
        }
        BooleanRetryTime booleanRetryTime = getBooleanRetryTime(cfIsSubscribed);
        if (booleanRetryTime == null || !booleanRetryTime.b()) {
            return;
        }
        if (!cfIsConnected.isDone()) {
            return;
        }
        booleanRetryTime = getBooleanRetryTime(cfIsConnected);
        if (booleanRetryTime == null
                || booleanRetryTime.retryTime() == null
                || booleanRetryTime.retryTime().isAfter(ZonedDateTime.now())) {
            return;
        }
        if (logger != null) {
            logger.debug(prefix + "Check connected");
        }
        cfIsConnected = quikConnect.futureResponseCB(
                "isConnected", (List<?>) null,
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
                logger.log(AbstractLogger.ERROR, prefix + "Cannot check isConnected() == 1", t);
            }
            connectedSince = null;
            return new BooleanRetryTime(
                    false,
                    ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS)
            );
        });
    }
}
