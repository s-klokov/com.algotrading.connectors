package com.algotrading.connectors.quik;

import com.simpleutils.quik.QuikConnect;

public class TradingTerminal {

    private final TradingQuikListener tradingQuikListener;
    private final QuikConnect quikConnect;

    public TradingTerminal(final TradingQuikListener tradingQuikListener, final QuikConnect quikConnect) {
        this.tradingQuikListener = tradingQuikListener;
        this.quikConnect = quikConnect;
        tradingQuikListener.setQuikConnect(quikConnect);
    }

    public void start() {
        quikConnect.start();
    }

    public void step() {
        processRunnables();
        tradingQuikListener.ensureConnection();
        tradingQuikListener.ensureSubscription();
    }

    public void processRunnables() {
        Runnable runnable;
        while ((runnable = tradingQuikListener.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                tradingQuikListener.logError("Cannot execute a runnable submitted by "
                        + tradingQuikListener.getClass().getSimpleName(), e);
            }
        }
    }

    public void shutdown() {
        quikConnect.shutdown();
        processRunnables();
    }
}
