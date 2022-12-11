package com.algotrading.connectors.quik;

import com.simpleutils.quik.QuikConnect;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TradingTerminal {

    private final TradingQuikListener tradingQuikListener;
    private final QuikConnect quikConnect;
    private final String terminalId;

    public TradingTerminal(final TradingQuikListener tradingQuikListener,
                           final QuikConnect quikConnect,
                           final String terminalId) {
        this.tradingQuikListener = tradingQuikListener;
        this.quikConnect = quikConnect;
        this.terminalId = terminalId;
        tradingQuikListener.setQuikConnect(quikConnect);
    }

    public String getTerminalId() {
        return terminalId;
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

    public boolean isOnline() {
        return tradingQuikListener.isOnline();
    }

    public boolean isSynchronized() {
        try {
            return isOnline()
                    && Boolean.TRUE.equals(quikConnect.executeMN(
                    "ServerInfo.isSynchronized", null,
                    tradingQuikListener.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS).get("result"));
        } catch (final ExecutionException e) {
            return false;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
