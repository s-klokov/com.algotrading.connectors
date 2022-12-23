package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.quik.ClassSecCode;
import com.simpleutils.quik.QuikConnect;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Collection;
import java.util.Set;
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

    public boolean isSubscribed() {
        return marketDataQuikListener.isSubscribed();
    }

    public boolean isSynchronized() throws ExecutionException, InterruptedException {
        return marketDataQuikListener.isSynchronized();
    }

    public boolean isCurrSynchronized() {
        return marketDataQuikListener.isCurrSynchronized();
    }

    public boolean isPrevSynchronized() {
        return marketDataQuikListener.isPrevSynchronized();
    }

    public JSONObject getSecurityInfo(final String classCode,
                                      final String secCode) throws ExecutionException, InterruptedException {
        return marketDataQuikListener.getSecurityInfo(classCode, secCode);
    }

    public JSONObject getParams(final String classCode,
                                final String secCode,
                                final Collection<String> parameters) throws ExecutionException, InterruptedException {
        return marketDataQuikListener.getParams(classCode, secCode, parameters);
    }

    public JSONObject getCandles(final String classCode,
                                 final String secCode,
                                 final int interval,
                                 final int maxSize) throws ExecutionException, InterruptedException {
        return marketDataQuikListener.getCandles(classCode, secCode, interval, maxSize);
    }

    public JSONObject getQuoteLevel2(final String classCode,
                                     final String secCode) throws ExecutionException, InterruptedException {
        return marketDataQuikListener.getQuoteLevel2(classCode, secCode);
    }

    public JSONArray getQuoteLevel2(final Set<ClassSecCode> classSecCodes) throws ExecutionException, InterruptedException {
        return marketDataQuikListener.getQuoteLevel2(classSecCodes);
    }
}
