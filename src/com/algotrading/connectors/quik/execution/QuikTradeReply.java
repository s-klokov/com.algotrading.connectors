package com.algotrading.connectors.quik.execution;

import java.util.Objects;

import static com.simpleutils.Deduplicator.STRING_DEDUPLICATOR;

/**
 * Сделка в терминале QUIK.
 */
public class QuikTradeReply {
    /**
     * Момент получения ответа (System.currentTimeMillis()).
     */
    public final long time;
    /**
     * Лимитная заявка, для которой пришёл ответ, или {@code null},
     * если пока связь с лимитной заявкой не установлена.
     */
    public QuikLimitOrder quikLimitOrder;
    /**
     * Инструмент.
     */
    public final String security;
    /**
     * Код класса.
     */
    public final String classCode;
    /**
     * Код инструмента.
     */
    public final String secCode;
    /**
     * Цена.
     */
    public final String price;
    /**
     * Размер лота.
     */
    public final int lotSize;
    /**
     * Объём сделки: положительное число -- покупка, отрицательное -- продажа.
     */
    public final int volumeTraded;
    /**
     * Номер заявки на бирже или 0, если неизвестен.
     */
    public long orderNum;
    /**
     * Номер сделки на бирже.
     */
    public final long tradeNum;
    /**
     * Размер комиссии.
     */
    public final double commission;
    /**
     * Биржевое время сделки.
     */
    public final long tradeTimeCode;
    /**
     * Дата расчётов по сделке или 0, если неизвестна.
     */
    public final int settleDate;

    public QuikTradeReply(final long time,
                          final QuikLimitOrder quikLimitOrder,
                          final String classCode,
                          final String secCode,
                          final String price,
                          final int lotSize,
                          final int volumeTraded,
                          final long orderNum,
                          final long tradeNum,
                          final double commission,
                          final long tradeTimeCode,
                          final int settleDate) {
        this.time = time;
        this.quikLimitOrder = quikLimitOrder;
        this.classCode = STRING_DEDUPLICATOR.deduplicate(Objects.requireNonNull(classCode));
        this.secCode = STRING_DEDUPLICATOR.deduplicate(Objects.requireNonNull(secCode));
        security = STRING_DEDUPLICATOR.deduplicate(classCode + ":" + secCode);
        this.price = price;
        this.lotSize = lotSize;
        this.volumeTraded = volumeTraded;
        this.orderNum = orderNum;
        this.tradeNum = tradeNum;
        this.commission = commission;
        this.tradeTimeCode = tradeTimeCode;
        this.settleDate = settleDate;
    }
}
