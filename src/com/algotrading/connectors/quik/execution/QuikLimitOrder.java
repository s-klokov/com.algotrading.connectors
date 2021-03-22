package com.algotrading.connectors.quik.execution;

import java.util.Objects;

import static com.algotrading.base.util.Deduplicator.STRING_DEDUPLICATOR;

/**
 * Лимитная заявка в терминале QUIK.
 */
public class QuikLimitOrder {
    /**
     * Номер транзакции терминала QUIK для выставления заявки.
     */
    public final int transId;
    /**
     * Номер заявки на бирже.
     */
    public long orderNum;
    /**
     * Позиция, к которой относится заявка.
     */
    public final QuikPosition quikPosition;
    /**
     * Цена заявки в строковом формате.
     */
    public final String price;
    /**
     * Размер лота.
     */
    public final int lotSize;
    /**
     * Размер заявки в лотах/контрактах.
     */
    public final int volume;
    /**
     * Оставшееся количество лотов/конктрактов в заявке.
     */
    public int volumeLeft;
    /**
     * Исполненное количество лотов/контрактов в заявке.
     */
    public int volumeTraded;
    /**
     * Время создания заявки (System.currentTimeMillis()).
     */
    public final long timeCreated;
    /**
     * Время для отмены заявки (System.currentTimeMillis()).
     */
    public final long timeToCancel;
    /**
     * Статус заявки.
     */
    public QuikLimitStatus status;
    /**
     * Время изменения статуса заявки (System.currentTimeMillis()).
     */
    public long statusTime;
    /**
     * Комментарий.
     */
    public final String brokerRef;
    /**
     * Код ошибки или 0, если ошибки нет.
     */
    public int errorCode;
    /**
     * Описание ошибки или {@code null}, если ошибки нет.
     */
    public String errorDescription;

    public QuikLimitOrder(final int transId,
                          final QuikPosition quikPosition,
                          final String price,
                          final int lotSize,
                          final int volume,
                          final long timeCreated,
                          final long timeToCancel,
                          final String brokerRef) {
        this.transId = transId;
        orderNum = 0;
        this.quikPosition = Objects.requireNonNull(quikPosition);
        this.price = Objects.requireNonNull(price);
        this.lotSize = lotSize;
        this.volume = volume;
        volumeLeft = volume;
        volumeTraded = 0;
        this.timeCreated = timeCreated;
        this.timeToCancel = timeToCancel;
        status = QuikLimitStatus.PENDING;
        statusTime = timeCreated;
        this.brokerRef = STRING_DEDUPLICATOR.deduplicate(Objects.requireNonNull(brokerRef));
        errorCode = 0;
        errorDescription = null;
    }

    public String security() {
        return quikPosition.security;
    }

    public String classCode() {
        return quikPosition.classCode;
    }

    public String secCode() {
        return quikPosition.secCode;
    }
}
