package com.algotrading.connectors.quik.execution;

import com.algotrading.connectors.quik.QuikClient;

import static com.simpleutils.Deduplicator.STRING_DEDUPLICATOR;

/**
 * Позиция.
 */
public class QuikPosition {
    public final String id;
    public final QuikClient quikClient;
    public final String security;
    public final String classCode;
    public final String secCode;
    public int lotSize;
    public int size;
    public double value;
    public double cash;

    public QuikPosition(final String id,
                        final QuikClient quikClient,
                        final String security,
                        final int lotSize,
                        final int size,
                        final double value,
                        final double cash) {
        this.id = id;
        this.quikClient = quikClient;
        this.security = STRING_DEDUPLICATOR.deduplicate(security);
        final int j = security.indexOf(':');
        classCode = STRING_DEDUPLICATOR.deduplicate(security.substring(0, j));
        secCode = STRING_DEDUPLICATOR.deduplicate(security.substring(j + 1));
        this.lotSize = lotSize;
        this.size = size;
        this.value = value;
        this.cash = cash;
    }
}
