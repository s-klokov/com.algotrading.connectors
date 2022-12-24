package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.quik.QuikConnect;
import org.json.simple.JSONObject;

import java.util.concurrent.ExecutionException;

public class TradingTerminal {

    private final TradingQuikListener tradingQuikListener;
    private final QuikConnect quikConnect;
    private final String terminalId;

    public static TradingTerminal newInstance(final AbstractLogger logger, final JSONObject config) {
        final TradingQuikListener tradingQuikListener = new TradingQuikListener();
        tradingQuikListener.configurate(logger, config);
        final QuikConnect quikConnect = QuikConnect.newInstance(config, tradingQuikListener);
        return new TradingTerminal(tradingQuikListener, quikConnect,
                JSONConfig.getString(config, "clientId"));
    }

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

    public boolean isSubscribed() {
        return tradingQuikListener.isSubscribed();
    }

    public boolean isSynchronized() throws ExecutionException, InterruptedException {
        return tradingQuikListener.isSynchronized();
    }

    public boolean isSynchronizedChanged() {
        return tradingQuikListener.isSynchronizedChanged();
    }
}
