package com.algotrading.connectors.common;

/**
 * Котировка в стакане.
 */
public class QuoteEntry {
    /**
     * Цена.
     */
    public final double price;
    /**
     * Количество.
     */
    public final int quantity;

    public QuoteEntry(final double price, final int quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "QuoteEntry{" + quantity + '@' + price + '}';
    }
}
