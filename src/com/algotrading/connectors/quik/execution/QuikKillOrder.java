package com.algotrading.connectors.quik.execution;

import java.util.Objects;

/**
 * Kill-заявка в терминале QUIK.
 */
public class QuikKillOrder {
    /**
     * Номер транзакции терминала QUIK для отправки kill-заявки.
     */
    public final int transId;
    /**
     * Снимаемая лимитная заявка.
     */
    public final QuikLimitOrder quikLimitOrder;
    /**
     * Момент создания kill-заявки.
     */
    public final long timeCreated;
    /**
     * Статус kill-заявки.
     */
    public QuikKillStatus status;
    /**
     * Момент изменения статуса заявки.
     */
    public long statusTime;
    /**
     * Код ошибки или 0, если ошибки нет.
     */
    public int errorCode;
    /**
     * Описание ошибки или {@code null}, если ошибки нет.
     */
    public String errorDescription;

    public QuikKillOrder(final int transId,
                         final QuikLimitOrder quikLimitOrder,
                         final long timeCreated) {
        this.transId = transId;
        this.quikLimitOrder = Objects.requireNonNull(quikLimitOrder);
        this.timeCreated = timeCreated;
        status = QuikKillStatus.PENDING;
        statusTime = timeCreated;
        errorCode = 0;
        errorDescription = null;
    }

    public String security() {
        return quikLimitOrder.security();
    }

    public String classCode() {
        return quikLimitOrder.classCode();
    }

    public String secCode() {
        return quikLimitOrder.secCode();
    }
}
