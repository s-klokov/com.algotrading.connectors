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
     * Объект для синхронизации.
     */
    private final Object mutex = new Object();
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
    private volatile QuikConnect quikConnect = null;
    /**
     * Поток для исполнения бизнес-логики.
     */
    private final Thread executionThread;
    /**
     * Таймаут при выполнении запросов и получении ответов от терминала.
     */
    private volatile long responseTimeoutMillis = 5000L;
    /**
     * Периодичность проверки наличия подключения терминала QUIK к серверу QUIK.
     */
    private volatile long checkConnectedTimeoutMillis = 5000L;
    /**
     * Периодичность попыток подписки на коллбэк OnDisconnected.
     */
    private volatile long failedSubscriptionTimeoutMillis = 5000L;

    private volatile boolean isRunning = true;
    private final Queue<Runnable> queue = new LinkedBlockingDeque<>();
    private ZonedDateTime connectedSince = null; // synchronized(mutex)

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
                executeRunnables();
                if (quikConnect.hasErrorMN() || quikConnect.hasErrorCB()) {
                    pause(quikConnect.errorSleepTimeout);
                } else {
                    ensureSubscription();
                    checkConnected();
                    pause(quikConnect.idleSleepTimeout);
                }
                if (Thread.currentThread().isInterrupted()) {
                    isRunning = false;
                    off();
                }
            }
        });
        executionThread.setName(clientId + "-" + QuikServerConnectionStatus.class.getSimpleName());
    }

    private static void pause(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
    public Thread getExecutionThread() {
        return executionThread;
    }

    @Override
    public void execute(final Runnable runnable) {
        queue.add(runnable);
    }

    /**
     * @return момент времени, начиная с которого имеется подключение терминала QUIK к серверу QUIK;
     * {@code null}, если подключение отсутствует
     */
    public ZonedDateTime connectedSince() {
        synchronized (mutex) {
            return connectedSince;
        }
    }

    private void executeRunnables() {
        Runnable runnable;
        while ((runnable = queue.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                if (logger != null) {
                    logger.log(AbstractLogger.ERROR, e.getMessage(), e);
                }
            }
        }
    }

    private void on() {
        nextSubscriptionTime = ZonedDateTime.now();
    }

    private void off() {
        synchronized (mutex) {
            connectedSince = null;
        }
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
        execute(this::on);
    }

    @Override
    public void onClose() {
        if (!isRunning) {
            return;
        }
        if (logger != null) {
            logger.info(prefix + "onClose");
        }
        execute(this::off);
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
        execute(() -> {
            synchronized (mutex) {
                connectedSince = null;
            }
            nextCheckConnectionTime = ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS);
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
        execute(this::off);
    }

    @Override
    public void onExceptionCB(final Exception exception) {
        if (!isRunning) {
            return;
        }
        if (logger != null) {
            logger.warn(prefix + "onExceptionCB");
        }
        execute(this::off);
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
                    responseTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            if (logger != null) {
                logger.log(AbstractLogger.ERROR, prefix + "Cannot subscribe to OnDisconnected", e);
            }
            nextSubscriptionTime = ZonedDateTime.now().plus(failedSubscriptionTimeoutMillis, ChronoUnit.MILLIS);
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
            nextSubscriptionTime = ZonedDateTime.now().plus(failedSubscriptionTimeoutMillis, ChronoUnit.MILLIS);
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
                    responseTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            if (logger != null) {
                logger.log(AbstractLogger.ERROR, "Cannot check isConnected() == 1", e);
            }
            nextCheckConnectionTime = ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        if ((long) result(response) == 1L) {
            final ZonedDateTime previous;
            synchronized (mutex) {
                previous = connectedSince;
                if (previous == null) {
                    connectedSince = ZonedDateTime.now();
                }
            }
            if (previous == null) {
                if (logger != null) {
                    logger.debug(prefix + "connected");
                }
            }
        } else {
            final ZonedDateTime prevConnectedSince;
            synchronized (mutex) {
                prevConnectedSince = connectedSince;
                connectedSince = null;
            }
            if (prevConnectedSince != null) {
                if (logger != null) {
                    logger.debug(prefix + "disconnected");
                }
            }
        }
        nextCheckConnectionTime = ZonedDateTime.now().plus(checkConnectedTimeoutMillis, ChronoUnit.MILLIS);
    }
}
