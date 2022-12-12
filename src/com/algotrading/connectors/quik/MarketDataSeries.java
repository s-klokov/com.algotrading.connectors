package com.algotrading.connectors.quik;

import com.algotrading.base.core.TimeCodes;
import com.algotrading.base.core.candles.UpdatableCandles;
import com.algotrading.base.core.series.FinSeries;
import com.simpleutils.json.JSONConfig;
import org.json.simple.JSONObject;

import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

public class MarketDataSeries {

    public final String seriesId;
    public final String clientId;
    public final String candlesId;
    public final String classCode;
    public final String secCode;
    public final int interval;
    public final int[] updateSizes;
    public final UpdatableCandles updatableCandles;

    public MarketDataSeries(final JSONObject config) {
        seriesId = JSONConfig.getString(config, "seriesId");
        clientId = JSONConfig.getString(config, "clientId");
        candlesId = JSONConfig.getString(config, "candlesId");
        String[] parts = candlesId.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Illegal candlesId=" + candlesId);
        }
        classCode = parts[0];
        secCode = parts[1];
        interval = Integer.parseInt(parts[2]);
        final LongUnaryOperator timeShift;
        if (config.get("timeShift") instanceof String timeShiftString) {
            timeShift = getTimeShiftFromString(timeShiftString);
        } else {
            timeShift = FinSeries.NO_TIME_SHIFT;
        }

        final LongPredicate timeFilter;
        if (config.get("timeFilter") instanceof String timeFilterString) {
            timeFilter = getTimeFilterFromString(timeFilterString);
        } else {
            timeFilter = FinSeries.ALL;
        }

        final int timeframe;
        final TimeUnit unit;
        if (config.get("compress") instanceof String compress) {
            switch (compress) {
                case "1m" -> {
                    timeframe = 1;
                    unit = TimeUnit.MINUTES;
                }
                case "5m" -> {
                    timeframe = 5;
                    unit = TimeUnit.MINUTES;
                }
                case "10m" -> {
                    timeframe = 10;
                    unit = TimeUnit.MINUTES;
                }
                case "15m" -> {
                    timeframe = 15;
                    unit = TimeUnit.MINUTES;
                }
                case "30m" -> {
                    timeframe = 30;
                    unit = TimeUnit.MINUTES;
                }
                case "60m", "1H" -> {
                    timeframe = 60;
                    unit = TimeUnit.MINUTES;
                }
                default -> throw new IllegalArgumentException("Illegal compress: " + compress);
            }
        } else {
            timeframe = Integer.parseInt(parts[2]);
            unit = TimeUnit.MINUTES;
        }

        parts = JSONConfig.getString(config, "updateSizes").split(",");
        updateSizes = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            updateSizes[i] = Integer.parseInt(parts[i]);
        }
        final int truncationSize = JSONConfig.getInt(config, "truncationSize");
        final int targetSize = JSONConfig.getInt(config, "targetSize");
        updatableCandles = new UpdatableCandles(timeShift, timeFilter, timeframe, unit, truncationSize, targetSize);
    }

    private static LongUnaryOperator getTimeShiftFromString(final String s) {
        return t -> t;
    }

    private static LongPredicate getTimeFilterFromString(final String s) {
        if (s == null) {
            return t -> true;
        }
        return switch (s) {
            case "[0900,1850)" -> between(900, 1850);
            case "[1000,1840)" -> between(1000, 1840);
            case "[1000,1850)" -> between(1000, 1850);
            default -> (t -> true);
        };
    }

    private static LongPredicate between(final int hhmmFrom, final int hhmmTill) {
        return t -> {
            final int hhmm = TimeCodes.hhmm(t);
            return hhmmFrom <= hhmm && hhmm < hhmmTill;
        };
    }
}
