package com.algotrading.connectors.common;

import java.util.List;

/**
 * Стакан котировок.
 */
public class QuoteLevel2 {
    public final List<QuoteEntry> bids;
    public final List<QuoteEntry> offers;

    public QuoteLevel2(final List<QuoteEntry> bids, final List<QuoteEntry> offers) {
        this.bids = bids;
        this.offers = offers;
    }

    public QuoteEntry getBid() {
        if (bids.isEmpty()) {
            return null;
        }
        QuoteEntry bid = bids.get(0);
        for (final QuoteEntry quoteEntry : bids) {
            if (bid.price() < quoteEntry.price()) {
                bid = quoteEntry;
            }
        }
        return bid;
    }

    public QuoteEntry getOffer() {
        if (offers.isEmpty()) {
            return null;
        }
        QuoteEntry offer = offers.get(0);
        for (final QuoteEntry quoteEntry : offers) {
            if (offer.price() > quoteEntry.price()) {
                offer = quoteEntry;
            }
        }
        return offer;
    }

    @Override
    public String toString() {
        return "QuoteLevel2{bid=" + getBid() + ", offer=" + getOffer() + '}';
    }
}
