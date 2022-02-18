package com.algotrading.connectors.quik;

import java.time.ZonedDateTime;

/**
 * Класс для хранения логического значения и момента времени,
 * после которого требуется пересчёт логического значения.
 * Если в качестве момента времени используется {@code null},
 * пересчёт логического значения не нужен.
 */
public record BooleanRetryTime(boolean b, ZonedDateTime retryTime) {

    public static final BooleanRetryTime FALSE_NO_RETRY = new BooleanRetryTime(false, null);
    public static final BooleanRetryTime TRUE_NO_RETRY = new BooleanRetryTime(true, null);
}
