package com.algotrading.connectors.quik;

import com.algotrading.base.core.columns.DoubleColumn;
import com.algotrading.base.core.columns.LongColumn;
import com.algotrading.base.core.series.FinSeries;
import com.algotrading.base.core.series.LongToLongFunction;
import com.algotrading.base.helpers.ParseHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.function.LongPredicate;

/**
 * Преобразование JSON-объектов, полученных из QUIK, в более удобный формат.
 */
public class QuikDecoder {

    /**
     * Получить булев статус ответа из терминала QUIK.
     *
     * @param response ответ терминала
     * @return статус
     */
    public static boolean status(final JSONObject response) {
        return ParseHelper.asBoolean(response.get("status"));
    }

    /**
     * Получить описание ошибки в ответе из терминала QUIK.
     *
     * @param response ответ терминала
     * @return описание ошибки или {@code null}, если его нет
     */
    public static String err(final JSONObject response) {
        return String.valueOf(response.get("err"));
    }

    /**
     * Получить результат из ответа из терминала QUIK.
     *
     * @param response ответ терминала
     * @return результат
     * @throws IllegalStateException с описанием ошибки, если статус {@code false} или отсутствует
     */
    public static Object result(final JSONObject response) {
        if (status(response)) {
            return response.get("result");
        } else {
            throw new IllegalStateException(err(response));
        }
    }

    /**
     * Преобразовать json-представление свечей в объект типа FinSeries
     * со сдвигом по времени, фильтрацией и компрессией.
     *
     * @param jsonCandles json-объект, полученный из QUIK
     * @param timeShift   временной сдвиг
     * @param timeFilter  фильтр по времени свечи
     * @return преобразованный временной ряд
     */
    public static FinSeries getCandles(final JSONObject jsonCandles,
                                       final LongToLongFunction timeShift,
                                       final LongPredicate timeFilter) {
        try {
            final int size = (int) ParseHelper.asLong(jsonCandles.get("size"));
            final JSONArray arrayT = (JSONArray) jsonCandles.get("T");
            final JSONArray arrayO = (JSONArray) jsonCandles.get("O");
            final JSONArray arrayH = (JSONArray) jsonCandles.get("H");
            final JSONArray arrayL = (JSONArray) jsonCandles.get("L");
            final JSONArray arrayC = (JSONArray) jsonCandles.get("C");
            final JSONArray arrayV = (JSONArray) jsonCandles.get("V");
            final FinSeries series = FinSeries.newCandles();
            final LongColumn timeCode = series.timeCode();
            final DoubleColumn open = series.open();
            final DoubleColumn high = series.high();
            final DoubleColumn low = series.low();
            final DoubleColumn close = series.close();
            final LongColumn volume = series.volume();
            for (int i = 0; i < size; i++) {
                final String timestamp = (String) arrayT.get(i);
                long t = parseTimestamp(timestamp);
                t = timeShift.applyAsLong(t);
                if (timeFilter.test(t)) {
                    timeCode.append(t);
                    open.append(ParseHelper.asDouble(arrayO.get(i)));
                    high.append(ParseHelper.asDouble(arrayH.get(i)));
                    low.append(ParseHelper.asDouble(arrayL.get(i)));
                    close.append(ParseHelper.asDouble(arrayC.get(i)));
                    volume.append((long) ParseHelper.asDouble(arrayV.get(i)));
                }
            }
            return series;
        } catch (final Exception e) {
            throw new IllegalArgumentException("Illegal JSON", e);
        }
    }

    private static final int TIMESTAMP_MASK = 0b11110110110110110110111;

    /**
     * Получить метку времени из timestamp-строки вида
     * 2020-11-25T05:15:00.000 или 2020-11-25T05:15:00
     *
     * @param timestamp строка даты-времени
     * @return метка времени в формате long
     */
    public static long parseTimestamp(final String timestamp) {
        final int len = timestamp.length();
        if (len != 19 && len != 23) {
            throw new IllegalArgumentException("Illegal timestamp: " + timestamp);
        }
        long t = 0L;
        for (int i = 0, m = 0b10000000000000000000000; i < len; i++, m >>= 1) {
            if ((TIMESTAMP_MASK & m) != 0) {
                final long d = timestamp.charAt(i) - '0';
                if (0L <= d && d <= 9L) {
                    t = t * 10L + d;
                } else {
                    throw new IllegalArgumentException("Illegal timestamp: " + timestamp);
                }
            }
        }
        if (len == 19) {
            t *= 1000L;
        }
        return t;
    }
}
