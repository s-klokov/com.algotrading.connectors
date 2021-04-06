package com.algotrading.connectors.quik;

import com.algotrading.base.core.candles.UpdatableCandles;
import com.algotrading.base.core.series.FinSeries;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.algotrading.base.core.series.FinSeries.ALL;
import static com.algotrading.base.core.series.FinSeries.NO_TIME_SHIFT;
import static com.algotrading.base.helpers.ParseHelper.asDouble;
import static com.algotrading.base.helpers.ParseHelper.asLong;

/**
 * Реализация хранилища обновляемых свечей терминала QUIK.
 * <p>
 * Инициализация объектов DataSource в терминале производится вызовом метода
 * {@link #initDataSources(long, TimeUnit)}.
 * <p>
 * После инициализации обновление свечей по данным из объектов DataSource производится
 * путём вызова метода {@link #updateDataSourceCandles(QuikDataSourceCandles, long, TimeUnit)},
 * а также есть опция использовать информацию из коллбэков, подписка на которые происходит
 * вызовом метода {@link #subscribeToOnCandle(long, TimeUnit)}, после чего обновление
 * свечных данных будет производиться при получении коллбэков.
 * <p>
 * Если планируется обновление свечных данных через получение коллбэков OnCandle,
 * то рекомендуется использовать следующий алгоритм заказа данных:<br>
 * 1) стартуя с пустого временного ряда и отсутствия подписки на коллбэк OnCandle,
 * инициализировать объект DataSource;<br>
 * 2) дождаться, чтобы в DataSource-объекте появилось ненулевое количество свечей;<br>
 * 3) подписаться на коллбэк OnCandle и дождаться успешного результата (после этого
 * начнут приходить коллбэки OnCandle);<br>
 * 4) произвести обновление объекта {@link QuikDataSourceCandles} с помощью метода
 * {@link #updateDataSourceCandles}, получив весь набор свечей, имеющийся к текущему моменту;<br>
 * 5) дальнейшие обновления объекта {@link QuikDataSourceCandles} будут происходить
 * при получении коллбэков.
 */
public class QuikCandlesStorage implements QuikInterface {
    /**
     * Объект типа {@link QuikInterface}, используемый для отправки и приёма сообщений.
     */
    private final QuikInterface quikInterface;
    /**
     * Хранилище объектов для работы со свечами из DataSource терминала QUIK.
     */
    public final Map<String, QuikDataSourceCandles> dataSourcesMap = new HashMap<>();
    /**
     * Последний результат инициализации DataSource-объектов.
     */
    public CompletableFuture<JSONObject> lastInitDataSourcesResult = null;
    /**
     * Последний результат запроса количества свечей в каждом из DataSource-объектов.
     */
    public CompletableFuture<JSONObject> lastDataSourceSizesResult = null;
    /**
     * Последний результат подписки на коллбэк OnCandle.
     */
    public CompletableFuture<Boolean> lastSubscribeResult = null;

    private final Collection<CompletableFuture<?>> activeFutures = new ArrayList<>();
    protected boolean isInterrupted = true;
    private boolean isRunning = false;

    public QuikCandlesStorage(final QuikInterface quikInterface) {
        this.quikInterface = quikInterface;
    }

    public void add(final QuikDataSourceCandles quikDataSourceCandles) {
        dataSourcesMap.put(quikDataSourceCandles.getDefaultKey(), quikDataSourceCandles);
    }

    /**
     * Выполнить настройку на основании массива строк вида:<br>
     * {@code "classCode:secCode:timeframe:[updateSize1,...,updateSizeN]:truncationSize:targetSize"}
     *
     * @param config конфигурация
     */
    @Override
    public void configurate(final JSONAware config) {
        for (final Object o : (JSONArray) config) {
            final String s = (String) o;
            final String[] parts = s.split(":");
            if (parts.length != 7) {
                throw new IllegalArgumentException("Illegal entry: " + s);
            }
            final String s2 = parts[2];
            final int period = Integer.parseInt(s2.substring(0, s2.length() - 1));
            final TimeUnit unit;
            switch (s2.charAt(s2.length() - 1)) {
                case 'd':
                case 'D':
                    unit = TimeUnit.DAYS;
                    break;
                case 'h':
                case 'H':
                    unit = TimeUnit.HOURS;
                    break;
                case 'm':
                case 'M':
                    unit = TimeUnit.MINUTES;
                    break;
                case 's':
                case 'S':
                    unit = TimeUnit.SECONDS;
                    break;
                default:
                    throw new IllegalArgumentException("Illegal timeframe: " + s2);
            }
            final String s3 = parts[3];
            if (!s3.startsWith("[") || !s3.endsWith("]")) {
                throw new IllegalArgumentException("Invalid update sizes: " + s3);
            }
            final String[] sizes = s3.substring(1, s3.length() - 1).split(",");
            final int[] updateSizes = new int[sizes.length];
            for (int j = 0; j < sizes.length; j++) {
                updateSizes[j] = Integer.parseInt(sizes[j]);
            }
            final int truncationSize = Integer.parseInt(parts[4]);
            final int targetSize = Integer.parseInt(parts[5]);
            final boolean isCallbackUsed = Boolean.parseBoolean(parts[6]);
            final QuikDataSourceCandles quikDataSourceCandles = new QuikDataSourceCandles(
                    parts[0], parts[1], period, unit, updateSizes, truncationSize, targetSize, isCallbackUsed
            );
            add(quikDataSourceCandles);
        }
    }

    @Override
    public void init() {
        isInterrupted = false;
        isRunning = true;
    }

    @Override
    public void shutdown() {
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
        if (this.isInterrupted && activeFutures.isEmpty()) {
            isRunning = false;
            return;
        }
        activeFutures.removeIf(CompletableFuture::isDone);
    }

    private static int quikPeriod(final QuikDataSourceCandles quikDataSourceCandles) {
        final UpdatableCandles updatableCandles = quikDataSourceCandles.updatableCandles;
        switch (updatableCandles.unit) {
            case MINUTES:
                return updatableCandles.period;
            case HOURS:
                return 60 * updatableCandles.period;
            default:
                throw new IllegalArgumentException("Illegal period: " + updatableCandles.period
                                                   + " " + updatableCandles.unit);
        }
    }

    /**
     * Инициализировать DataSource-объекты в QUIK.
     * <p>
     * Возвращает будущий JSON-объект, где для каждого ключа набора свечей указано "ok"
     * в случае успешной инициализации и описание ошибки в случае неудачи.
     *
     * @param timeout таймаут запроса
     * @param unit    единица измерения времени
     * @return объект типа {@link CompletableFuture<JSONObject>}
     */
    public CompletableFuture<JSONObject> initDataSources(final long timeout, final TimeUnit unit) {
        final StringBuilder sb = new StringBuilder();
        sb.append("return { ");
        try (final Formatter f = new Formatter(sb)) {
            dataSourcesMap.forEach(
                    (k, v) -> f.format("[\"%s\"] = initDataSource(\"%s\", \"%s\", %d), ",
                                       k, v.classCode, v.secCode, quikPeriod(v)));
        }
        sb.append("}");
        lastInitDataSourcesResult =
                getResponseMN(sb.toString(), timeout, unit)
                        .thenApply(response -> (JSONObject) QuikDecoder.result(response));
        activeFutures.add(lastInitDataSourcesResult);
        return lastInitDataSourcesResult;
    }

    /**
     * Отправить запрос о количестве свечей в каждом используемом DataSource-объекте.
     * <p>
     * Возвращает будущий JSON-объект, где для каждого ключа набора свечей указано количество свечей
     * в соответствующем ему DataSource-объекте.
     *
     * @param timeout таймаут запроса
     * @param unit    единица измерения времени
     * @return объект типа {@link CompletableFuture<JSONObject>}
     */
    public CompletableFuture<JSONObject> getDataSourceSizes(final long timeout, final TimeUnit unit) {
        final StringBuilder sb = new StringBuilder();
        sb.append("return { ");
        try (final Formatter f = new Formatter(sb)) {
            dataSourcesMap.forEach(
                    (k, v) -> f.format("[\"%s\"] = getDataSourceSize(\"%s\", \"%s\", %d), ",
                                       k, v.classCode, v.secCode, quikPeriod(v)));
        }
        sb.append("}");
        lastDataSourceSizesResult = getResponseMN(sb.toString(), timeout, unit)
                .thenApply(response -> (JSONObject) QuikDecoder.result(response));
        activeFutures.add(lastDataSourceSizesResult);
        return lastDataSourceSizesResult;
    }

    /**
     * Обновить набор свечей.
     * <p>
     * Попытки обновления производятся с использованием возрастающего
     * количества свечей из массива {@link QuikDataSourceCandles#updateSizes} до тех пор,
     * пока массив не закончится или не будет получен результат, отличный от -1.
     * <p>
     * Если текущее количество свечей менее 10, то обновление производится однократно
     * с использованием последнего (самого большого) элемента массива
     * {@link QuikDataSourceCandles#updateSizes}.
     *
     * @param quikDataSourceCandles набор обновляемых свечей
     * @param timeout               таймаут запроса
     * @param unit                  единица измерения времени
     * @return будущий результат, возвращаемый методом {@link QuikDataSourceCandles#update(FinSeries)}
     */
    public CompletableFuture<Integer> updateDataSourceCandles(final QuikDataSourceCandles quikDataSourceCandles,
                                                              final long timeout, final TimeUnit unit) {
        final int[] updateSizes = quikDataSourceCandles.updateSizes;
        final int start = (quikDataSourceCandles.length() >= 10) ? 0 : (updateSizes.length - 1);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(-1);
        for (int i = start; i < updateSizes.length; i++) {
            final int updateSize = updateSizes[i];
            final CompletableFuture<Integer> f = future;
            future = f.thenCompose(result -> {
                if (result >= 0) {
                    return f;
                }
                return getResponseMN(
                        "getCandles",
                        List.of(quikDataSourceCandles.classCode,
                                quikDataSourceCandles.secCode,
                                quikPeriod(quikDataSourceCandles),
                                updateSize),
                        timeout, unit
                ).thenApply(response -> {
                    final JSONObject jsonCandles = (JSONObject) QuikDecoder.result(response);
                    final FinSeries newSeries = QuikDecoder.getCandles(jsonCandles, NO_TIME_SHIFT, ALL);
                    return quikDataSourceCandles.update(newSeries);
                });
            });
        }
        activeFutures.add(future);
        return future;
    }

    /**
     * Произвести подписку на коллбэк OnCandle.
     *
     * @param timeout таймаут запроса
     * @param unit    единица измерения времени
     * @return будущий результат
     */
    public CompletableFuture<Boolean> subscribeToOnCandle(final long timeout, final TimeUnit unit) {
        lastSubscribeResult = getResponseCB(
                "OnCandle", "function() return true end", timeout, unit
        ).thenApply(response -> {
            if (QuikDecoder.status(response)) {
                return true;
            } else {
                throw new IllegalStateException("Cannot subscribe to OnCandle callback: " + QuikDecoder.err(response));
            }
        });
        activeFutures.add(lastSubscribeResult);
        return lastSubscribeResult;
    }

    /**
     * Отменить подписку на коллбэк OnCandle.
     *
     * @param timeout таймаут запроса
     * @param unit    единица измерения времени
     * @return будущий результат
     */
    public CompletableFuture<Boolean> unsubscribeFromOnCandle(final long timeout, final TimeUnit unit) {
        lastSubscribeResult = getResponseCB(
                "OnCandle", "function() return false end", timeout, unit
        ).thenApply(response -> {
            if (QuikDecoder.status(response)) {
                return true;
            } else {
                throw new IllegalStateException("Cannot unsubscribe from OnCandle callback: " + QuikDecoder.err(response));
            }
        });
        activeFutures.add(lastSubscribeResult);
        return lastSubscribeResult;
    }

    @Override
    public void onOpen() {
        lastInitDataSourcesResult = null;
        lastDataSourceSizesResult = null;
        lastSubscribeResult = null;
    }

    @Override
    public void onClose() {
    }

    @Override
    public void onReceiveMN(final JSONObject jsonObject) {
    }

    @Override
    public void onReceiveCB(final JSONObject jsonObject) {
        final String name = (String) jsonObject.get("callback");
        if (!"OnCandle".equals(name)) {
            return;
        }
        final JSONObject candle = (JSONObject) jsonObject.get("arg1");

        final String classCode = (String) candle.get("class_code");
        final String secCode = (String) candle.get("sec_code");
        final long timeframe = (Long) candle.get("timeframe");
        String key = classCode + ":" + secCode + ":" + timeframe + "m";
        QuikDataSourceCandles quikDataSourceCandles = dataSourcesMap.get(key);
        if (quikDataSourceCandles == null && timeframe >= 60) {
            key = classCode + ":" + secCode + ":" + (timeframe / 60) + "H";
            quikDataSourceCandles = dataSourcesMap.get(key);
        }
        if (quikDataSourceCandles != null && quikDataSourceCandles.isCallbackUsed) {
            quikDataSourceCandles.update(
                    QuikDecoder.parseTimestamp((String) candle.get("T")),
                    asDouble(candle.get("O")),
                    asDouble(candle.get("H")),
                    asDouble(candle.get("L")),
                    asDouble(candle.get("C")),
                    asLong(candle.get("V"))
            );
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
