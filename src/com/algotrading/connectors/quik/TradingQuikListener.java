package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.quik.SimpleQuikListener;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.simpleutils.json.JSONConfig.getLong;

public class TradingQuikListener extends SimpleQuikListener {

    public final Map<Long, JSONObject> transReplyMap = new HashMap<>();
    private long uid = 0L;
    private JSONArray brokerRefs = new JSONArray();

    public void configurate(final AbstractLogger logger, final JSONObject config) {
        setLogger(logger);
        uid = JSONConfig.getLong(config, "uid");
        brokerRefs = JSONConfig.getJSONArray(config, "brokerRefs");
        setLogPrefix(JSONConfig.getStringNonNull(config, "clientId") + ": ");

        configurate(config, "requestTimeout", this::setRequestTimeout);
        configurate(config, "pauseAfterException", this::setPauseAfterException);
        configurate(config, "checkConnectedPeriod", this::setCheckConnectedPeriod);
        configurate(config, "subscriptionPeriod", this::setSubscriptionPeriod);
        configurate(config, "onlineDuration", this::setOnlineDuration);

        configurateCallbacks((JSONArray) config.get("callbacks"));
    }

    private void configurate(final JSONObject config, final String key, final Consumer<Duration> consumer) {
        if (config.containsKey(key)) {
            consumer.accept(Duration.of(JSONConfig.getLong(config, key), ChronoUnit.MILLIS));
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
            case "OnTransReply" -> onTransReply((JSONObject) jsonObject.get("arg1"));
            case "OnOrder" -> onOrder((JSONObject) jsonObject.get("arg1"));
            case "OnStopOrder" -> onStopOrder((JSONObject) jsonObject.get("arg1"));
            case "OnTrade" -> onTrade((JSONObject) jsonObject.get("arg1"));
            default -> super.processCallback(callback, jsonObject);
        }
    }

    protected void onTransReply(final JSONObject jsonObject) {
        if (jsonObject == null || getLong(jsonObject, "uid") != uid) {
            return;
        }
        final long transId = getLong(jsonObject, "trans_id");
        if (transId == 0L) {
            return;
        }
        logger.debug(() -> logPrefix + "OnTransReply: " + jsonObject);
        transReplyMap.put(transId, jsonObject);
    }

    protected void onOrder(final JSONObject jsonObject) {
        if (jsonObject == null || getLong(jsonObject, "uid") != uid) {
            return;
        }
        logger.debug(() -> logPrefix + "OnOrder: " + jsonObject);
    }

    protected void onStopOrder(final JSONObject jsonObject) {
        if (jsonObject == null || getLong(jsonObject, "uid") != uid) {
            return;
        }
        logger.debug(() -> logPrefix + "OnStopOrder: " + jsonObject);
    }

    protected void onTrade(final JSONObject jsonObject) {
        if (jsonObject == null || jsonObject.get("uid") == null || getLong(jsonObject, "uid") != uid) {
            return;
        }
        logger.debug(() -> logPrefix + "OnTrade: " + jsonObject);
    }
}
