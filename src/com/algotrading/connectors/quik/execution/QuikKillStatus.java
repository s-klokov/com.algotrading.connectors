package com.algotrading.connectors.quik.execution;

public enum QuikKillStatus {
    /**
     * Kill-заявка ожидает отправки на биржу в очереди.
     */
    PENDING,
    /**
     * Kill-заявка отправлена на биржу.
     */
    SENT,
    /**
     * Kill-заявка исполнена.
     */
    EXECUTED,
    /**
     * Произошла ошибка.
     */
    ERROR,
}
