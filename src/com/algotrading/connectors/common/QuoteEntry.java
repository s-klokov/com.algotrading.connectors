package com.algotrading.connectors.common;

/**
 * Котировка в стакане.
 *
 * @param price    цена
 * @param quantity количество
 */
public record QuoteEntry(double price, int quantity) {

    @Override
    public String toString() {
        return "QuoteEntry{" + quantity + '@' + price + '}';
    }
}
