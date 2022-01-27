package com.algotrading.connectors.quik;

import com.algotrading.base.util.SocketConnector;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.json.simple.JSONArray.toJSONString;
import static org.json.simple.JSONValue.escape;

/**
 * Подключение к терминалу QUIK через сокеты.
 */
public class QuikConnect {
    /**
     * Кодовая страница для текстовых сообщений.
     */
    public Charset charset = Charset.forName("CP1251");
    /**
     * Таймаут в миллисекундах перед повторной попыткой открыть сокеты в случае возникновения ошибок.
     */
    public long errorTimeout = 60_000L;
    /**
     * Периодичность отправки ping-сообщений в миллисекундах.
     */
    public long pingTimeout = 15_000L;
    /**
     * Длительность паузы рабочего цикла в миллисекундах в случае отсутствия сообщений.
     */
    public long idleSleepTimeout = 10L;
    /**
     * Длительность паузы рабочего цикла в миллисекундах в случае ошибок.
     */
    public long errorSleepTimeout = 100L;

    /**
     * Счётчик для номеров сообщений.
     */
    private final AtomicLong counter = new AtomicLong();
    /**
     * Соответствие между номерами запросов и ответами на них.
     */
    private final Map<Long, CompletableFuture<JSONObject>> responseMap = new HashMap<>();
    /**
     * Socket-коннектор к MN-серверу.
     */
    private final SocketConnector scMN;
    /**
     * Socket-коннектор к CB-серверу.
     */
    private final SocketConnector scCB;
    /**
     * Идентификатор клиента.
     */
    public final String clientId;
    /**
     * Слушатель событий от терминала QUIK.
     */
    public final QuikListener listener;
    /**
     * Поток, который слушает ответы от терминала QUIK.
     */
    private final Thread listeningThread;

    /**
     * Парсер json-строк.
     */
    private final JSONParser parser = new JSONParser();
    /**
     * Признак того, что socket-коннекторы открыты.
     */
    private boolean hasOpenSocketConnectors = false;
    /**
     * Признак ошибки при взаимодействии с MN-сервером.
     */
    private volatile boolean hasErrorMN = false;
    /**
     * Признак ошибки при взаимодействии с CB-сервером.
     */
    private volatile boolean hasErrorCB = false;
    /**
     * Момент возникновения ошибки взаимодействия с терминалом.
     */
    private volatile long errorTime = 0L;
    /**
     * Момент отправки сообщения "ping".
     */
    private long lastPingTime = 0L;
    /**
     * Счётчик сообщений в одной итерации рабочего цикла.
     */
    private int count = 0;

    /**
     * Конструктор.
     *
     * @param host     хост
     * @param portMN   порт MN-сервера
     * @param portCB   порт CB-сервера
     * @param clientId идентификатор клиента
     */
    public QuikConnect(final String host, final int portMN, final int portCB,
                       final String clientId,
                       final QuikListener listener) {
        scMN = new SocketConnector(host, portMN);
        scCB = new SocketConnector(host, portCB);
        this.clientId = clientId;
        this.listener = listener;
        listeningThread = new Thread() {
            @Override
            public void run() {
                while (!interrupted()) {
                    step();
                }
            }

            private void step() {
                if (hasErrorMN || hasErrorCB) {
                    if (hasOpenSocketConnectors) {
                        closeSocketConnectors();
                    }
                    if (System.currentTimeMillis() >= errorTime + errorTimeout) {
                        hasErrorMN = false;
                        hasErrorCB = false;
                    }
                }
                if (hasErrorMN || hasErrorCB) {
                    pause(errorSleepTimeout);
                    cleanupResponseMap();
                    return;
                }
                if (!hasOpenSocketConnectors) {
                    synchronized (scMN) {
                        try {
                            scMN.open(charset);
                        } catch (final IOException e) {
                            hasErrorMN = true;
                            errorTime = System.currentTimeMillis();
                            try {
                                listener.onExceptionMN(e);
                            } catch (final Exception ignored) {
                            }
                        }
                    }
                    synchronized (scCB) {
                        try {
                            scCB.open(charset);
                        } catch (final IOException e) {
                            hasErrorCB = true;
                            errorTime = System.currentTimeMillis();
                            try {
                                listener.onExceptionCB(e);
                            } catch (final Exception ignored) {
                            }
                        }
                    }
                    if (hasErrorMN || hasErrorCB) {
                        hasErrorMN = true;
                        hasErrorCB = true;
                        closeSocketConnectors();
                        cleanupResponseMap();
                        return;
                    }
                    hasOpenSocketConnectors = true;
                    try {
                        listener.onOpen();
                    } catch (final Exception ignored) {
                    }
                }

                ensurePing();

                count = 0;
                if (!hasErrorMN) {
                    receiveMN();
                }
                if (!hasErrorCB) {
                    receiveCB();
                }
                cleanupResponseMap();
                if (count == 0) {
                    pause(idleSleepTimeout);
                }
            }

            private void receiveMN() {
                while (true) {
                    final String s;
                    synchronized (scMN) {
                        try {
                            s = scMN.receive();
                        } catch (final IOException e) {
                            hasErrorMN = true;
                            errorTime = System.currentTimeMillis();
                            try {
                                listener.onExceptionMN(e);
                            } catch (final Exception ignored) {
                            }
                            break;
                        }
                    }
                    if (s == null) {
                        break;
                    }
                    count++;
                    if ("pong".equals(s)) {
                        continue;
                    }
                    try {
                        final JSONObject jsonObject = (JSONObject) parser.parse(s);
                        // TODO: _onReceiveMN();
                    } catch (final ParseException | ClassCastException e) {
                        try {
                            listener.onExceptionMN(e);
                        } catch (final Exception ignored) {
                        }
                    }
                }
            }

            private void receiveCB() {
                while (true) {
                    final String s;
                    synchronized (scCB) {
                        try {
                            s = scCB.receive();
                        } catch (final IOException e) {
                            hasErrorCB = true;
                            errorTime = System.currentTimeMillis();
                            try {
                                listener.onExceptionCB(e);
                            } catch (final Exception ignored) {
                            }
                            break;
                        }
                    }
                    if (s == null) {
                        break;
                    }
                    count++;
                    if ("pong".equals(s)) {
                        continue;
                    }
                    try {
                        final JSONObject jsonObject = (JSONObject) parser.parse(s);
                        // TODO: _onReceiveCB();
                    } catch (final ParseException | ClassCastException e) {
                        try {
                            listener.onExceptionCB(e);
                        } catch (final Exception ignored) {
                        }
                    }
                }
            }

            private void cleanupResponseMap() {
                synchronized (responseMap) {
                    responseMap.entrySet().removeIf(entry -> entry.getValue().isDone());
                }
            }

            private void pause(final long millis) {
                try {
                    Thread.sleep(millis);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            private void ensurePing() {
                final long now = System.currentTimeMillis();
                if (now >= lastPingTime + pingTimeout) {
                    synchronized (scMN) {
                        if (!hasErrorMN) {
                            try {
                                scMN.send("ping");
                            } catch (final IOException e) {
                                hasErrorMN = true;
                                errorTime = System.currentTimeMillis();
                            }
                        }
                    }
                    synchronized (scCB) {
                        if (!hasErrorCB) {
                            try {
                                scCB.send("ping");
                            } catch (final IOException e) {
                                hasErrorCB = true;
                                errorTime = System.currentTimeMillis();
                            }
                        }
                    }
                    lastPingTime = now;
                }
            }

            private void closeSocketConnectors() {
                synchronized (scMN) {
                    try {
                        scMN.send("quit");
                    } catch (final IOException ignored) {
                    } finally {
                        scMN.close();
                    }
                }
                synchronized (scCB) {
                    try {
                        scCB.send("quit");
                    } catch (final IOException ignored) {
                    } finally {
                        scCB.close();
                    }
                }
                hasOpenSocketConnectors = false;
                try {
                    listener.onClose();
                } catch (final Exception ignored) {
                }
            }
        };
        listeningThread.setName("Listener-" + clientId);
    }

    public void start() {
        listeningThread.start();
    }

    public boolean hasErrorMN() {
        return hasErrorMN;
    }

    public boolean hasErrorCB() {
        return hasErrorCB;
    }

    public void shutdown() {
        listeningThread.interrupt();
        try {
            listeningThread.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendSyncMN(final String s) throws IOException {
        synchronized (scMN) {
            try {
                scMN.send(s);
            } catch (final IOException e) {
                hasErrorMN = true;
                errorTime = System.currentTimeMillis();
                throw e;
            }
        }
    }

    private void sendSyncCB(final String s) throws IOException {
        synchronized (scCB) {
            try {
                scCB.send(s);
            } catch (final IOException e) {
                hasErrorCB = true;
                errorTime = System.currentTimeMillis();
                throw e;
            }
        }
    }

    private long sendMN(final String chunk) throws IOException {
        final long id = counter.incrementAndGet();
        sendSyncMN("{\"id\":" + id
                + ",\"clientId\":\"" + clientId
                + "\",\"chunk\":\"" + escape(chunk) + "\"}");
        return id;
    }

    private long sendMN(final String fname, final List<?> args) throws IOException {
        final long id = counter.incrementAndGet();
        sendSyncMN("{\"id\":" + id
                + ",\"clientId\":\"" + clientId
                + "\",\"fname\":\"" + fname
                + "\",\"args\":" + toJSONString(args) + "}");
        return id;
    }

    private long sendCB(final String chunk) throws IOException {
        final long id = counter.incrementAndGet();
        sendSyncCB("{\"id\":" + id
                + ",\"clientId\":\"" + clientId
                + "\",\"chunk\":\"" + escape(chunk) + "\"}");

        return id;
    }

    private long sendCB(final String fname, final List<?> args) throws IOException {
        final long id = counter.incrementAndGet();
        sendSyncCB("{\"id\":" + id
                + ",\"clientId\":\"" + clientId
                + "\",\"fname\":\"" + fname
                + "\",\"args\":" + toJSONString(args) + "}");
        return id;
    }

    private long sendCB(final String callback, final String filter) throws IOException {
        final long id = counter.incrementAndGet();
        sendSyncCB("{\"id\":" + id
                + ",\"clientId\":\"" + clientId
                + "\",\"callback\":\"" + callback
                + "\",\"filter\":\"" + escape(filter) + "\"}");
        return id;
    }

    /**
     * Отправить chunk-запрос MN-серверу и получить будущий ответ.
     *
     * @param chunk   код запроса на языке QLua
     * @param timeout таймаут ожидания
     * @param unit    единица измерения времени
     * @return будущий ответ MN-сервера
     */
    public CompletableFuture<JSONObject> getResponseMN(final String chunk,
                                                       final long timeout, final TimeUnit unit) {
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            final long id = sendMN(chunk);
            response = response.orTimeout(timeout, unit);
            synchronized (responseMap) {
                responseMap.put(id, response);
            }
        } catch (final IOException e) {
            response.completeExceptionally(e);
        }
        return response;
    }

    /**
     * Отправить function-запрос MN-серверу и получить будущий ответ.
     *
     * @param fname   имя QLua-функции
     * @param args    список аргументов функции
     * @param timeout таймаут ожидания
     * @param unit    единица измерения времени
     * @return будущий ответ MN-сервера
     */
    public CompletableFuture<JSONObject> getResponseMN(final String fname, final List<?> args,
                                                       final long timeout, final TimeUnit unit) {
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            final long id = sendMN(fname, args);
            response = response.orTimeout(timeout, unit);
            synchronized (responseMap) {
                responseMap.put(id, response);
            }
        } catch (final IOException e) {
            response.completeExceptionally(e);
        }
        return response;
    }

    /**
     * Отправить chunk-запрос CB-серверу и получить будущий ответ.
     *
     * @param chunk   код запроса на языке QLua
     * @param timeout таймаут ожидания
     * @param unit    единица измерения времени
     * @return будущий ответ CB-сервера
     */
    public CompletableFuture<JSONObject> getResponseCB(final String chunk,
                                                       final long timeout, final TimeUnit unit) {
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            final long id = sendCB(chunk);
            response = response.orTimeout(timeout, unit);
            synchronized (responseMap) {
                responseMap.put(id, response);
            }
        } catch (final IOException e) {
            response.completeExceptionally(e);
        }
        return response;
    }

    /**
     * Отправить function-запрос CB-серверу и получить будущий ответ.
     *
     * @param fname   имя QLua-функции
     * @param args    список аргументов функции
     * @param timeout таймаут ожидания
     * @param unit    единица измерения времени
     * @return будущий ответ CB-сервера
     */
    public CompletableFuture<JSONObject> getResponseCB(final String fname, final List<?> args,
                                                       final long timeout, final TimeUnit unit) {
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            final long id = sendCB(fname, args);
            response = response.orTimeout(timeout, unit);
            synchronized (responseMap) {
                responseMap.put(id, response);
            }
        } catch (final IOException e) {
            response.completeExceptionally(e);
        }
        return response;
    }

    /**
     * Отправить запрос CB-серверу и получить будущий ответ.
     *
     * @param callback имя коллбэка
     * @param filter   код функции фильтрации на языке QLua
     * @param timeout  таймаут ожидания
     * @param unit     единица измерения времени
     * @return будущий ответ CB-сервера
     */
    public CompletableFuture<JSONObject> getResponseCB(final String callback, final String filter,
                                                       final long timeout, final TimeUnit unit) {
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            final long id = sendCB(callback, filter);
            response = response.orTimeout(timeout, unit);
            synchronized (responseMap) {
                responseMap.put(id, response);
            }
        } catch (final IOException e) {
            response.completeExceptionally(e);
        }
        return response;
    }
}
