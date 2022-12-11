package com.algotrading.connectors.quik;

import com.simpleutils.quik.QuikConnect;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MarketDataTerminal {

    private final MarketDataQuikListener marketDataQuikListener;
    private final QuikConnect quikConnect;
    private final String terminalId;

    public MarketDataTerminal(final MarketDataQuikListener marketDataQuikListener,
                              final QuikConnect quikConnect,
                              final String terminalId) {
        this.marketDataQuikListener = marketDataQuikListener;
        this.quikConnect = quikConnect;
        this.terminalId = terminalId;
        marketDataQuikListener.setQuikConnect(quikConnect);
    }

    public String getTerminalId() {
        return terminalId;
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
        while ((runnable = marketDataQuikListener.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                marketDataQuikListener.logError("Cannot execute a runnable submitted by "
                        + marketDataQuikListener.getClass().getSimpleName(), e);
            }
        }
    }

    public void shutdown() {
        quikConnect.shutdown();
        processRunnables();
    }

    public boolean isOnline() {
        return marketDataQuikListener.isOnline();
    }

    public boolean isSynchronized() {
        try {
            return isOnline()
                    && Boolean.TRUE.equals(quikConnect.executeMN(
                    "ServerInfo.isSynchronized", null,
                    marketDataQuikListener.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS).get("result"));
        } catch (final ExecutionException e) {
            return false;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
