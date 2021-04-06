package com.algotrading.connectors.quik;

import com.algotrading.base.util.SocketConnector;
import org.json.simple.JSONAware;
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

import static com.algotrading.base.util.JSONConfig.getOrDefault;
import static org.json.simple.JSONArray.toJSONString;
import static org.json.simple.JSONValue.escape;

/**
 * Базовый класс для агентов, взаимодействующих с одним терминалом QUIK.
 */
public class QuikAgent implements QuikInterface {
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
     * Признак исполнения рабочего цикла.
     */
    private boolean isRunning = false;
    /**
     * Признак прерывания.
     */
    public boolean isInterrupted = true;
    /**
     * Номер сообщения.
     */
    private long id = 0;
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
    private boolean hasErrorMN = false;
    /**
     * Признак ошибки при взаимодействии с CB-сервером.
     */
    private boolean hasErrorCB = false;
    /**
     * Момент возникновения ошибки взаимодействия с терминалом.
     */
    private long errorTime = 0L;
    /**
     * Момент отправки сообщения "ping".
     */
    private long lastPingTime = 0L;
    /**
     * Счётчик сообщений в одной итерации рабочего цикла.
     */
    private int count = 0;
    /**
     * Последняя ошибка ввода-вывода.
     */
    private IOException lastIOException = null;
    /**
     * Соответствие между номерами запросов и ответами на них.
     */
    private final Map<Long, CompletableFuture<JSONObject>> responseMap = new HashMap<>();

    /**
     * Конструктор.
     *
     * @param host     хост
     * @param portMN   порт MN-сервера
     * @param portCB   порт CB-сервера
     * @param clientId идентификатор клиента
     */
    public QuikAgent(final String host, final int portMN, final int portCB, final String clientId) {
        scMN = new SocketConnector(host, portMN);
        scCB = new SocketConnector(host, portCB);
        this.clientId = clientId;
    }

    @Override
    public void configurate(final JSONAware config) {
        final JSONObject json = (JSONObject) config;
        charset = Charset.forName(getOrDefault(json, "charset", "CP1251"));
        errorTimeout = getOrDefault(json, "errorTimeout", errorTimeout);
        pingTimeout = getOrDefault(json, "pingTimeout", pingTimeout);
        idleSleepTimeout = getOrDefault(json, "idleSleepTimeout", idleSleepTimeout);
        errorSleepTimeout = getOrDefault(json, "errorSleepTimeout", errorSleepTimeout);
    }

    @Override
    public void init() {
        isInterrupted = false;
        isRunning = true;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public boolean hasErrorMN() {
        return hasErrorMN;
    }

    @Override
    public boolean hasErrorCB() {
        return hasErrorCB;
    }

    @Override
    public void step(final boolean isInterrupted, final boolean hasErrorMN, final boolean hasErrorCB) {
        if (isInterrupted) {
            this.isInterrupted = true;
        }
        if (this.isInterrupted) {
            isRunning = false;
        }
    }

    /**
     * Запустить цикл работы агента.
     * <p>
     * Прерывание агента происходит присваиванием переменной {@link #isInterrupted} значения
     * {@code true}, после чего агент самостоятельно через некоторое время заканчивает работу,
     * функция {@link #isRunning()} возвращает значение {@code false} и цикл завершается.
     */
    public void run() {
        while (isRunning()) {
            runIteration();
        }
    }

    /**
     * Выполнить одну итерацию цикла работы агента. См. метод {@link #run()}.
     */
    public void runIteration() {
        if (hasErrorMN() || hasErrorCB()) {
            if (hasOpenSocketConnectors) {
                closeSocketConnectors();
            }
            if (System.currentTimeMillis() >= errorTime + errorTimeout) {
                hasErrorMN = false;
                hasErrorCB = false;
            }
        }
        if (hasErrorMN() || hasErrorCB()) {
            pause(errorSleepTimeout);
            step(isInterrupted, hasErrorMN(), hasErrorCB());
            cleanupResponseMap();
            return;
        }
        if (!hasOpenSocketConnectors) {
            try {
                scMN.open(charset);
            } catch (final IOException e) {
                onIOExceptionMN(e);
            }
            try {
                scCB.open(charset);
            } catch (final IOException e) {
                onIOExceptionCB(e);
            }
            if (hasErrorMN() || hasErrorCB()) {
                hasErrorMN = true;
                hasErrorCB = true;
                closeSocketConnectors();
                step(isInterrupted, hasErrorMN(), hasErrorCB());
                cleanupResponseMap();
                return;
            }
            hasOpenSocketConnectors = true;
            onOpen();
        }

        ensurePing();

        count = 0;
        if (!hasErrorMN()) {
            receiveMN();
        }
        if (!hasErrorCB()) {
            receiveCB();
        }
        step(isInterrupted, hasErrorMN(), hasErrorCB());
        cleanupResponseMap();
        if (count == 0) {
            pause(idleSleepTimeout);
        }
    }

    @Override
    public void shutdown() {
        isInterrupted = true;
        isRunning = false;
        closeSocketConnectors();
    }

    protected void closeSocketConnectors() {
        try {
            scMN.send("quit");
        } catch (final IOException ignored) {
        } finally {
            scMN.close();
        }
        try {
            scCB.send("quit");
        } catch (final IOException ignored) {
        } finally {
            scCB.close();
        }
        hasOpenSocketConnectors = false;
        onClose();
    }

    /**
     * Вызывается после установления соединения с терминалом.
     */
    @Override
    public void onOpen() {
    }

    /**
     * Вызывается после закрытия соединения с терминалом.
     */
    @Override
    public void onClose() {
    }

    @Override
    public void onReceiveMN(final JSONObject jsonObject) {
        final Object o = jsonObject.get("id");
        if (o instanceof Long) {
            final CompletableFuture<JSONObject> response = responseMap.remove(o);
            if (response != null) {
                response.complete(jsonObject);
            }
        }
    }

    @Override
    public void onReceiveCB(final JSONObject jsonObject) {
        final Object o = jsonObject.get("id");
        if (o instanceof Long) {
            final CompletableFuture<JSONObject> response = responseMap.remove(o);
            if (response != null) {
                response.complete(jsonObject);
            }
        }
    }

    @Override
    public void onIOExceptionMN(final IOException e) {
        if (!hasErrorMN()) {
            hasErrorMN = true;
            errorTime = System.currentTimeMillis();
        }
        lastIOException = e;
    }

    @Override
    public void onIOExceptionCB(final IOException e) {
        if (!hasErrorCB()) {
            hasErrorCB = true;
            errorTime = System.currentTimeMillis();
        }
        lastIOException = e;
    }

    @Override
    public void onException(final Exception e) {
        e.printStackTrace();
    }

    @Override
    public void sendMN(final String chunk) throws IOException {
        scMN.send("{\"id\":" + (++id)
                  + ",\"clientId\":\"" + clientId
                  + "\",\"chunk\":\"" + escape(chunk) + "\"}");
        count++;
    }

    @Override
    public void sendMN(final String fname, final List<?> args) throws IOException {
        scMN.send("{\"id\":" + (++id)
                  + ",\"clientId\":\"" + clientId
                  + "\",\"fname\":\"" + fname
                  + "\",\"args\":" + toJSONString(args) + "}");
        count++;
    }

    @Override
    public void sendCB(final String chunk) throws IOException {
        scCB.send("{\"id\":" + (++id)
                + ",\"clientId\":\"" + clientId
                + "\",\"chunk\":\"" + escape(chunk) + "\"}");
        count++;
    }

    @Override
    public void sendCB(final String fname, final List<?> args) throws IOException {
        scCB.send("{\"id\":" + (++id)
                + ",\"clientId\":\"" + clientId
                + "\",\"fname\":\"" + fname
                + "\",\"args\":" + toJSONString(args) + "}");
        count++;
    }

    @Override
    public void sendCB(final String callback, final String filter) throws IOException {
        scCB.send("{\"id\":" + (++id)
                  + ",\"clientId\":\"" + clientId
                  + "\",\"callback\":\"" + callback
                  + "\",\"filter\":\"" + escape(filter) + "\"}");
        count++;
    }

    protected void receiveMN() {
        while (true) {
            final String s;
            try {
                s = scMN.receive();
            } catch (final IOException e) {
                onIOExceptionMN(e);
                break;
            }
            if (s == null) {
                break;
            }
            count++;
            if ("pong".equals(s)) {
                continue;
            }
            try {
                onReceiveMN((JSONObject) parser.parse(s));
            } catch (final ParseException | ClassCastException e) {
                onException(e);
            }
        }
    }

    protected void receiveCB() {
        while (true) {
            final String s;
            try {
                s = scCB.receive();
            } catch (final IOException e) {
                onIOExceptionCB(e);
                break;
            }
            if (s == null) {
                break;
            }
            count++;
            if ("pong".equals(s)) {
                continue;
            }
            try {
                onReceiveCB((JSONObject) parser.parse(s));
            } catch (final ParseException | ClassCastException e) {
                onException(e);
            }
        }
    }

    @Override
    public CompletableFuture<JSONObject> getResponseMN(final String chunk,
                                                       final long timeout, final TimeUnit unit) {
        if (hasErrorMN()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "ErrorMN has happened before the call", lastIOException));
        }
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            sendMN(chunk);
            response = response.orTimeout(timeout, unit);
            responseMap.put(id, response);
        } catch (final IOException e) {
            onIOExceptionMN(e);
            response.completeExceptionally(e);
        }
        return response;
    }

    @Override
    public CompletableFuture<JSONObject> getResponseMN(final String fname, final List<?> args,
                                                       final long timeout, final TimeUnit unit) {
        if (hasErrorMN()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "ErrorMN has happened before the call", lastIOException));
        }
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            sendMN(fname, args);
            response = response.orTimeout(timeout, unit);
            responseMap.put(id, response);
        } catch (final IOException e) {
            onIOExceptionMN(e);
            response.completeExceptionally(e);
        }
        return response;
    }

    @Override
    public CompletableFuture<JSONObject> getResponseCB(final String chunk,
                                                       final long timeout, final TimeUnit unit) {
        if (hasErrorCB()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "ErrorCB has happened before the call", lastIOException));
        }
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            sendCB(chunk);
            response = response.orTimeout(timeout, unit);
            responseMap.put(id, response);
        } catch (final IOException e) {
            onIOExceptionCB(e);
            response.completeExceptionally(e);
        }
        return response;
    }

    @Override
    public CompletableFuture<JSONObject> getResponseCB(final String fname, final List<?> args,
                                                       final long timeout, final TimeUnit unit) {
        if (hasErrorCB()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "ErrorCB has happened before the call", lastIOException));
        }
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            sendCB(fname, args);
            response = response.orTimeout(timeout, unit);
            responseMap.put(id, response);
        } catch (final IOException e) {
            onIOExceptionCB(e);
            response.completeExceptionally(e);
        }
        return response;
    }

    @Override
    public CompletableFuture<JSONObject> getResponseCB(final String callback, final String filter,
                                                       final long timeout, final TimeUnit unit) {
        if (hasErrorCB()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "ErrorCB has happened before the call", lastIOException));
        }
        CompletableFuture<JSONObject> response = new CompletableFuture<>();
        try {
            sendCB(callback, filter);
            response = response.orTimeout(timeout, unit);
            responseMap.put(id, response);
        } catch (final IOException e) {
            onIOExceptionCB(e);
            response.completeExceptionally(lastIOException);
        }
        return response;
    }

    private void pause(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            isInterrupted = true;
        }
    }

    private void ensurePing() {
        final long now = System.currentTimeMillis();
        if (now >= lastPingTime + pingTimeout) {
            if (!hasErrorMN()) {
                try {
                    scMN.send("ping");
                } catch (final IOException e) {
                    onIOExceptionMN(e);
                }
            }
            if (!hasErrorCB()) {
                try {
                    scCB.send("ping");
                } catch (final IOException e) {
                    onIOExceptionCB(e);
                }
            }
            lastPingTime = now;
        }
    }

    private void cleanupResponseMap() {
        responseMap.entrySet().removeIf(entry -> entry.getValue().isDone());
    }
}
