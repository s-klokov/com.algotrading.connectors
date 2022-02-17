package com.algotrading.connectors.quik.deprecated;

import com.algotrading.base.core.candles.UpdatableCandles;
import com.algotrading.base.core.series.FinSeries;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.algotrading.base.core.series.FinSeries.ALL;
import static com.algotrading.base.core.series.FinSeries.NO_TIME_SHIFT;

/**
 * Класс для работы со свечами, получаемыми из DataSource-объектов терминала QUIK.
 */
public class QuikDataSourceCandles {
    /**
     * Код класса.
     */
    public final String classCode;
    /**
     * Код инструмента.
     */
    public final String secCode;
    /**
     * Количество свечей, запрашиваемых из источника для обновления.
     * В массиве элементы идут по возрастанию: если после запроса
     * предыдущего количества свечей не удалось соединить уже имеющиеся
     * свечи с запрошенными, делается последующий запрос с увеличенным
     * количеством свечей.
     * <p>
     * В первоначальном запросе свечей используется количество, записанное
     * в последнем элементе массива.
     */
    public final int[] updateSizes;
    /**
     * Нужно ли использовать функцию обратного вызова об обновлении свечей.
     */
    public final boolean isCallbackUsed;
    /**
     * Обновляемые свечи.
     */
    public final UpdatableCandles updatableCandles;

    /**
     * Конструктор.
     *
     * @param classCode      код класса
     * @param secCode        код инструмента
     * @param period         таймфрейм
     * @param unit           единица измерения времени
     * @param updateSizes    массив с возрастающими количествами свечей, запрашиваемыми при обновлении
     * @param truncationSize граница количества свечей для урезания временного ряда
     * @param targetSize     количество свечей после урезация временного ряда
     */
    public QuikDataSourceCandles(final String classCode, final String secCode,
                                 final int period, final TimeUnit unit,
                                 final int[] updateSizes,
                                 final int truncationSize,
                                 final int targetSize,
                                 final boolean isCallbackUsed) {
        this.classCode = Objects.requireNonNull(classCode);
        this.secCode = Objects.requireNonNull(secCode);
        if (updateSizes == null || updateSizes.length == 0) {
            throw new IllegalArgumentException("Invalid updateSizes: " + Arrays.toString(updateSizes));
        }
        this.updateSizes = updateSizes;
        updatableCandles = new UpdatableCandles(NO_TIME_SHIFT, ALL, period, unit, truncationSize, targetSize);
        this.isCallbackUsed = isCallbackUsed;
    }

    /**
     * @return длина временного ряда
     */
    public int length() {
        return updatableCandles.length();
    }

    public int update(final FinSeries newSeries) {
        return updatableCandles.update(newSeries, false);
    }

    public int update(final long t, final double o, final double h, final double l, final double c, final long v) {
        return isCallbackUsed ? updatableCandles.update(t, o, h, l, c, v) : -1;
    }

    /**
     * @return строковый ключ
     */
    public String getDefaultKey() {
        final int period = updatableCandles.period;
        final TimeUnit unit = updatableCandles.unit;
        switch (unit) {
            case DAYS:
                if (period == 1) {
                    return classCode + ":" + secCode + ":D";
                } else {
                    throw new UnsupportedOperationException("Illegal daily period");
                }
            case HOURS:
                return classCode + ":" + secCode + ":" + period + "H";
            case MINUTES:
                return classCode + ":" + secCode + ":" + period + "m";
            case SECONDS:
                return classCode + ":" + secCode + ":" + period + "s";
            default:
                throw new UnsupportedOperationException("Illegal time unit " + unit);
        }
    }
}
