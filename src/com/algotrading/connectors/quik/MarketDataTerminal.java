package com.algotrading.connectors.quik;

import com.simpleutils.quik.QuikConnect;

public class MarketDataTerminal {

    private final MarketDataQuikListener marketDataQuikListener;
    private final QuikConnect quikConnect;

    public MarketDataTerminal(final MarketDataQuikListener marketDataQuikListener, final QuikConnect quikConnect) {
        this.marketDataQuikListener = marketDataQuikListener;
        this.quikConnect = quikConnect;
        marketDataQuikListener.setQuikConnect(quikConnect);
    }

    public void start() {
        quikConnect.start();
    }

    public void step() {
        processRunnables();
        marketDataQuikListener.ensureConnection();
        marketDataQuikListener.ensureSubscription();
    }

    public void processRunnables() {
        Runnable runnable;
        while ((runnable = marketDataQuikListener.queue.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                marketDataQuikListener.logError("Cannot execute a runnable from QuikListener", e);
            }
        }
    }

    public void shutdown() {
        quikConnect.shutdown();
        processRunnables();
    }
}
