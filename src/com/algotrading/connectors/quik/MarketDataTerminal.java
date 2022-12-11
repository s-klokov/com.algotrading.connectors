package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.quik.QuikConnect;
import org.json.simple.JSONObject;

import java.util.concurrent.ExecutionException;

public class MarketDataTerminal {

    private final MarketDataQuikListener marketDataQuikListener;
    private final QuikConnect quikConnect;
    private final String terminalId;

    public static MarketDataTerminal newInstance(final AbstractLogger logger, final JSONObject config) {
        final MarketDataQuikListener marketDataQuikListener = new MarketDataQuikListener();
        marketDataQuikListener.configurate(logger, config);
        final QuikConnect quikConnect = QuikConnect.newInstance(config, marketDataQuikListener);
        return new MarketDataTerminal(marketDataQuikListener, quikConnect,
                JSONConfig.getString(config, "clientId"));
    }

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

    public boolean isSynchronized() throws ExecutionException, InterruptedException {
        return marketDataQuikListener.isSynchronized();
    }
}
