package com.algotrading.connectors.quik;

import com.simpleutils.logs.AbstractLogger;
import org.json.simple.JSONObject;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.algotrading.base.util.JSONConfig.getLong;
import static com.algotrading.connectors.quik.QuikDecoder.result;
import static com.algotrading.connectors.quik.QuikDecoder.status;

/**
 * Отслеживание наличия подключения терминала QUIK к серверу QUIK.
 */
public class QuikServerConnectionStatus extends AbstractQuikListener {
    /**
     * Логгер.
     */
    private final AbstractLogger logger;
    /**
     * Префикс для лога.
     */
    private final String prefix;
    /**
     * Таймаут при выполнении запросов и получении ответов от терминала.
     */
    private volatile long responseTimeout = 5000L;
    /**
     * Периодичность проверки наличия подключения терминала QUIK к серверу QUIK.
     */
    private volatile long checkConnectedTimeout = 5000L;
    /**
     * Периодичность попыток подписки на коллбэк OnDisconnected.
     */
    private volatile long failedSubscriptionTimeout = 5000L;
    /**
     * Длительность паузы рабочего цикла в миллисекундах в случае отсутствия сообщений.
     */
    private volatile long idleSleepTimeout = 100L;
    /**
     * Длительность паузы рабочего цикла в миллисекундах в случае ошибок.
     */
    private volatile long errorSleepTimeout = 500L;

    private volatile boolean isRunning = true;
    private volatile ZonedDateTime connectedSince = null;

    private boolean isSubscribed = false;
    private ZonedDateTime nextSubscriptionTime = null;
    private ZonedDateTime nextCheckConnectionTime = null;

    /**
     * Конструктор.
     *
     * @param logger   логгер
     * @param clientId идентификатор клиента
     */
    public QuikServerConnectionStatus(final AbstractLogger logger, final String clientId) {
        this.logger = logger;
        prefix = clientId + ": " + QuikServerConnectionStatus.class.getSimpleName() + ": ";
        executionThread = new Thread(() -> {
            while (isRunning) {
                final int count = executeRunnables();
                if (quikConnect.hasErrorMN() || quikConnect.hasErrorCB()) {
                    pause(errorSleepTimeout);
                } else {
                    ensureSubscription();
                    checkConnected();
                    if (count == 0) {
                        pause(idleSleepTimeout);
                    }
                }
                if (Thread.currentThread().isInterrupted()) {
                    isRunning = false;
                    off();
                }
            }
        });
        executionThread.setName(clientId + "-" + QuikServerConnectionStatus.class.getSimpleName());
    }

    public QuikServerConnectionStatus withResponseTimeout(final long responseTimeout) {
        this.responseTimeout = responseTimeout;
        return this;
    }

    public QuikServerConnectionStatus withCheckConnectedTimeout(final long checkConnectedTimeout) {
        this.checkConnectedTimeout = checkConnectedTimeout;
        return this;
    }

    public QuikServerConnectionStatus withFailedSubscriptionTimeout(final long failedSubscriptionTimeout) {
        this.failedSubscriptionTimeout = failedSubscriptionTimeout;
        return this;
    }

    public QuikServerConnectionStatus withIdleSleepTimeout(final long idleSleepTimeout) {
        this.idleSleepTimeout = idleSleepTimeout;
        return this;
    }

    public QuikServerConnectionStatus withErrorSleepTimeout(final long errorSleepTimeout) {
        this.errorSleepTimeout = errorSleepTimeout;
        return this;
    }

    public long responseTimeout() {
        return responseTimeout;
    }

    public long checkConnectedTimeout() {
        return checkConnectedTimeout;
    }

    public long failedSubscriptionTimeout() {
        return failedSubscriptionTimeout;
    }

    public long idleSleepTimeout() {
        return idleSleepTimeout;
    }

    public long errorSleepTimeout() {
        return errorSleepTimeout;
    }

    public void configurate(final JSONObject json) {
        responseTimeout = getLong(json, "responseTimeout");
        checkConnectedTimeout = getLong(json, "checkConnectedTimeout");
        failedSubscriptionTimeout = getLong(json, "failedSubscriptionTimeout");
        idleSleepTimeout = getLong(json, "idleSleepTimeout");
        errorSleepTimeout = getLong(json, "errorSleepTimeout");
    }

    /**
     * @return момент времени, начиная с которого имеется подключение терминала QUIK к серверу QUIK;
     * {@code null}, если подключение отсутствует
     */
    public ZonedDateTime connectedSince() {
        return connectedSince;
    }

    /**
     * Выполнить код из очереди.
     *
     * @return количество исполненных блоков кода
     */
    private int executeRunnables() {
        int count = 0;
        Runnable runnable;
        while ((runnable = queue.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                if (logger != null) {
                    logger.log(AbstractLogger.ERROR, e.getMessage(), e);
                }
            }
            count++;
        }
        return count;
    }

    private void on() {
        nextSubscriptionTime = ZonedDateTime.now();
    }

    private void off() {
        connectedSince = null;
        isSubscribed = false;
        nextSubscriptionTime = null;
        nextCheckConnectionTime = null;
    }

    @Override
    public void onOpen() {
        if (!isRunning) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "onOpen");
        }
        submit(this::on);
    }

    @Override
    public void onClose() {
        if (!isRunning) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "onClose");
        }
        submit(this::off);
    }

    @Override
    public void onCallback(final JSONObject jsonObject) {
        if (!isRunning) {
            return;
        }
        if (!"OnDisconnected".equals(jsonObject.get("callback"))) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "OnDisconnected");
        }
        submit(() -> {
            connectedSince = null;
            nextCheckConnectionTime = ZonedDateTime.now().plus(checkConnectedTimeout, ChronoUnit.MILLIS);
        });
    }

    @Override
    public void onExceptionMN(final Exception exception) {
        if (!isRunning) {
            return;
        }
        if (logger != null) {
            logger.warn(prefix + "onExceptionMN");
        }
        submit(this::off);
    }

    @Override
    public void onExceptionCB(final Exception exception) {
        if (!isRunning) {
            return;
        }
        if (logger != null) {
            logger.warn(prefix + "onExceptionCB");
        }
        submit(this::off);
    }

    private void ensureSubscription() {
        if (isSubscribed
                || nextSubscriptionTime == null
                || nextSubscriptionTime.isAfter(ZonedDateTime.now())) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "Subscribe to OnDisconnect");
        }
        final JSONObject response;
        try {
            response = quikConnect.responseCB(
                    "OnDisconnected",
                    "*",
                    responseTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            if (logger != null) {
                logger.log(AbstractLogger.ERROR, prefix + "Cannot subscribe to OnDisconnected", e);
            }
            nextSubscriptionTime = ZonedDateTime.now().plus(failedSubscriptionTimeout, ChronoUnit.MILLIS);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        final boolean status = status(response);
        if (logger != null) {
            logger.info(prefix + "subscribed to OnDisconnected: " + status);
        }
        if (status) {
            isSubscribed = true;
            nextSubscriptionTime = null;
            nextCheckConnectionTime = ZonedDateTime.now();
        } else {
            if (logger != null) {
                logger.error(prefix + "Cannot subscribe to OnDisconnected");
            }
            nextSubscriptionTime = ZonedDateTime.now().plus(failedSubscriptionTimeout, ChronoUnit.MILLIS);
        }
    }

    private void checkConnected() {
        if (!isSubscribed
                || nextCheckConnectionTime == null
                || nextCheckConnectionTime.isAfter(ZonedDateTime.now())) {
            return;
        }
        if (logger != null) {
            logger.debug(prefix + "Check connected");
        }
        final JSONObject response;
        try {
            response = quikConnect.responseCB(
                    "isConnected", (List<?>) null,
                    responseTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            if (logger != null) {
                logger.log(AbstractLogger.ERROR, "Cannot check isConnected() == 1", e);
            }
            nextCheckConnectionTime = ZonedDateTime.now().plus(checkConnectedTimeout, ChronoUnit.MILLIS);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        if ((long) result(response) == 1L) {
            if (connectedSince == null) {
                connectedSince = ZonedDateTime.now();
                if (logger != null) {
                    logger.debug(prefix + "connected");
                }
            }
        } else {
            if (connectedSince != null) {
                connectedSince = null;
                if (logger != null) {
                    logger.debug(prefix + "disconnected");
                }
            }
        }
        nextCheckConnectionTime = ZonedDateTime.now().plus(checkConnectedTimeout, ChronoUnit.MILLIS);
    }
}
