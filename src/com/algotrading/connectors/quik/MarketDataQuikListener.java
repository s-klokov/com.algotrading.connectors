package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.quik.ClassSecCode;
import com.simpleutils.quik.SimpleQuikListener;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

public class MarketDataQuikListener extends SimpleQuikListener {

    public void configurate(final AbstractLogger logger, final JSONObject config) {
        setLogger(logger);
        setLogPrefix(JSONConfig.getStringNonNull(config, "terminalId") + ": ");

        configurate(config, "requestTimeout", this::setRequestTimeout);
        configurate(config, "pauseAfterException", this::setPauseAfterException);
        configurate(config, "checkConnectedPeriod", this::setCheckConnectedPeriod);
        configurate(config, "subscriptionPeriod", this::setSubscriptionPeriod);
        configurate(config, "onlineDuration", this::setOnlineDuration);

        configurateParams((JSONArray) config.get("params"));
        configurateCandles((JSONArray) config.get("candles"));
        configurateLevel2Quotes((JSONArray) config.get("level2Quotes"));
        configurateCallbacks((JSONArray) config.get("callbacks"));
    }

    private void configurate(final JSONObject config, final String key, final Consumer<Duration> consumer) {
        if (config.containsKey(key)) {
            consumer.accept(Duration.of(JSONConfig.getLong(config, key), ChronoUnit.MILLIS));
        }
    }

    private void configurateParams(final JSONArray array) {
        for (final Object o : array) {
            final String entry = (String) o;
            final String[] parts = entry.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Illegal params enrty: " + entry);
            }
            final ClassSecCode classSecCode = new ClassSecCode(parts[0], parts[1]);
            if (parts[2].length() >= 2 && parts[2].startsWith("[") && parts[2].endsWith("]")) {
                final String[] parameters = parts[2].substring(1, parts[2].length() - 1).split(",");
                addSecurityParameters(classSecCode, parameters);
            } else {
                throw new IllegalArgumentException("Illegal params entry: " + entry);
            }
        }
    }

    private void configurateCandles(final JSONArray array) {
        for (final Object o : array) {
            final String entry = (String) o;
            final String[] parts = entry.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Illegal candles entry: " + entry);
            }
            final ClassSecCode classSecCode = new ClassSecCode(parts[0], parts[1]);
            if (parts[2].length() >= 2 && parts[2].startsWith("[") && parts[2].endsWith("]")) {
                final String[] intervals = parts[2].substring(1, parts[2].length() - 1).split(",");
                for (final String interval : intervals) {
                    try {
                        addSecurityCandles(classSecCode, Integer.parseInt(interval));
                    } catch (final NumberFormatException e) {
                        throw new IllegalArgumentException("Illegal candles entry: " + entry);
                    }
                }
            } else {
                throw new IllegalArgumentException("Illegal candles entry: " + entry);
            }
        }
    }

    private void configurateLevel2Quotes(final JSONArray array) {
        for (final Object o : array) {
            final String entry = (String) o;
            final String[] parts = entry.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Illegal level2Quotes entry: " + entry);
            }
            addLevel2Quotes(new ClassSecCode(parts[0], parts[1]));
        }
    }

    private void configurateCallbacks(final JSONArray array) {
        for (final Object o : array) {
            final JSONObject json = (JSONObject) o;
            addCallbackSubscription(
                    JSONConfig.getStringNonNull(json, "callback"),
                    JSONConfig.getStringNonNull(json, "filter")
            );
        }
    }

    @Override
    protected void processCallback(final String callback, final JSONObject jsonObject) {
        switch (callback) {
            case "OnConnected" -> onConnected();
            case "OnDisconnected" -> onDisconnected();
            case "OnAllTrade" -> onAllTrade(jsonObject);
            default -> onUnknownCallback(callback);
        }
    }

    @Override
    protected void onUnknownCallback(final String callback) {
        logger.debug(logPrefix + "Unknown callback: " + callback);
    }

    protected void onAllTrade(final JSONObject jsonObject) {
        logger.debug(logPrefix + "OnAllTrade: " + jsonObject);
    }
}
