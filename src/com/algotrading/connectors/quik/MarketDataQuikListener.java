package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.quik.ClassSecCode;
import com.simpleutils.quik.SimpleQuikListener;
import com.simpleutils.quik.requests.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MarketDataQuikListener extends SimpleQuikListener {

    protected Map<ClassSecCode, Set<String>> securityParametersMap = new LinkedHashMap<>();
    protected Map<ClassSecCode, Set<Integer>> securityCandlesMap = new LinkedHashMap<>();
    protected Set<ClassSecCode> level2QuotesSet = new LinkedHashSet<>();

    public void addSecurityParameter(final ClassSecCode classSecCode, final String parameter) {
        securityParametersMap.computeIfAbsent(classSecCode, k -> new LinkedHashSet<>()).add(parameter);
    }

    public void addSecurityParameters(final ClassSecCode classSecCode, final String[] parameters) {
        Collections.addAll(securityParametersMap.computeIfAbsent(classSecCode, k -> new LinkedHashSet<>()), parameters);
    }

    public void addSecurityParameters(final ClassSecCode classSecCode, final Collection<String> parameters) {
        securityParametersMap.computeIfAbsent(classSecCode, k -> new LinkedHashSet<>()).addAll(parameters);
    }

    public void addSecurityCandles(final ClassSecCode classSecCode, final int interval) {
        securityCandlesMap.computeIfAbsent(classSecCode, k -> new LinkedHashSet<>()).add(interval);
    }

    public void addSecurityCandles(final ClassSecCode classSecCode, final int[] intervals) {
        final Set<Integer> set = securityCandlesMap.computeIfAbsent(classSecCode, k -> new LinkedHashSet<>());
        for (final int interval : intervals) {
            set.add(interval);
        }
    }

    public void addSecurityCandles(final ClassSecCode classSecCode, final Collection<Integer> intervals) {
        securityCandlesMap.computeIfAbsent(classSecCode, k -> new LinkedHashSet<>()).addAll(intervals);
    }

    public void addLevel2Quotes(final ClassSecCode classSecCode) {
        level2QuotesSet.add(classSecCode);
    }

    public void configurate(final AbstractLogger logger, final JSONObject config) {
        setLogger(logger);
        setLogPrefix(JSONConfig.getStringNonNull(config, "clientId") + ": ");

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
            case "OnAllTrade" -> onAllTrade((JSONObject) jsonObject.get("arg1"));
            case "OnQuote" -> onQuote(
                    (String) jsonObject.get("arg1"),
                    (String) jsonObject.get("arg2"),
                    jsonObject.get("result"));
            default -> super.processCallback(callback, jsonObject);
        }
    }

    protected void onAllTrade(final JSONObject jsonObject) {
        logger.debug(() -> logPrefix + "OnAllTrade: " + jsonObject);
    }

    protected void onQuote(final String classCode, final String secCode, final Object result) {
        logger.debug(() -> logPrefix + "OnQuote(" + classCode + "," + secCode + "): " + result);
    }

    @Override
    public void subscribe() {
        try {
            subscribeToParameters();
            subscribeToCandles();
            subscribeToLevel2Quotes();
            super.subscribe();
        } catch (final Exception e) {
            isSubscribed = false;
            nextSubscriptionTime = ZonedDateTime.now().plus(subscriptionPeriod);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void subscribeToParameters() throws ExecutionException, InterruptedException {
        for (final Map.Entry<ClassSecCode, Set<String>> entry : securityParametersMap.entrySet()) {
            subscribeToSecurityParameters(entry.getKey(), entry.getValue());
        }
    }

    private void subscribeToCandles() throws ExecutionException, InterruptedException {
        for (final Map.Entry<ClassSecCode, Set<Integer>> entry : securityCandlesMap.entrySet()) {
            subscribeToSecurityCandles(entry.getKey(), entry.getValue());
        }
    }

    private void subscribeToLevel2Quotes() throws ExecutionException, InterruptedException {
        if (level2QuotesSet.isEmpty()) {
            return;
        }
        final JSONObject response = quikConnect.executeMN(
                new BulkQuoteLevel2SubscriptionRequest(level2QuotesSet).getRequest(),
                requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        final JSONArray result = (JSONArray) response.get("result");
        String errorMessage = null;
        for (final Object o : result) {
            final JSONObject json = (JSONObject) o;
            if (Boolean.TRUE.equals(json.get("subscribed"))) {
                if (logger != null) {
                    logger.debug(() -> logPrefix + "Subscribed to Level2 quotes for " + json.get("classCode") + ":" + json.get("secCode") + ".");
                }
            } else {
                errorMessage = "Cannot subscribed to Level2 quotes for " + json.get("classCode") + ":" + json.get("secCode") + ".";
                if (logger != null) {
                    logger.debug(logPrefix + errorMessage);
                }
            }
        }
        if (errorMessage != null) {
            throw new RuntimeException(errorMessage);
        }
    }

    private void subscribeToSecurityParameters(final ClassSecCode classSecCode,
                                               final Collection<String> parameters) throws ExecutionException, InterruptedException {
        final JSONObject response = quikConnect.executeMN(
                new ParamSubscriptionRequest(classSecCode, parameters).getRequest(),
                requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (Boolean.TRUE.equals(response.get("result"))) {
            if (logger != null) {
                logger.debug(() -> logPrefix + "Subscribed to " + classSecCode + " " + parameters + ".");
            }
            return;
        }
        final String message = "Cannot subscribe to " + classSecCode + " parameters " + parameters + ".";
        if (logger != null) {
            logger.error(logPrefix + message);
            throw new RuntimeException(message);
        }
    }

    private void subscribeToSecurityCandles(final ClassSecCode classSecCode,
                                            final Collection<Integer> intervals) throws ExecutionException, InterruptedException {
        final JSONObject response = quikConnect.executeMN(
                new CandlesSubscriptionRequest(classSecCode, intervals).getRequest(),
                requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        try {
            final JSONObject result = (JSONObject) response.get("result");
            RuntimeException runtimeException = null;
            for (final int interval : intervals) {
                final String key = String.valueOf(interval);
                if (!"ok".equals(result.get(key))) {
                    final String message = "Cannot subscribe to " + classSecCode
                            + " candles for interval " + interval + ": " + result.get(key);
                    if (logger != null) {
                        logger.error(logPrefix + message);
                        runtimeException = new RuntimeException(message);
                    }
                }
            }
            if (runtimeException == null) {
                if (logger != null) {
                    logger.debug(() -> logPrefix + "Subscribed to " + classSecCode + " candles for intervals " + intervals + ".");
                }
            } else {
                throw runtimeException;
            }
        } catch (final NullPointerException | ClassCastException e) {
            final String message = "Cannot subscribe to " + classSecCode + " candles for intervals " + intervals + ".";
            if (logger != null) {
                logger.error(logPrefix + message);
                throw new RuntimeException(message);
            }
        }
    }

    public JSONObject getSecurityInfo(final String classCode,
                                      final String secCode) throws ExecutionException, InterruptedException {
        return (JSONObject) quikConnect.executeMN(
                "getSecurityInfo", List.of(classCode, secCode),
                requestTimeout.toMillis(), TimeUnit.MILLISECONDS).get("result");
    }

    public JSONObject getParams(final String classCode,
                                final String secCode,
                                final Collection<String> parameters) throws ExecutionException, InterruptedException {
        return (JSONObject) executeMN(new GetParamExRequest(classCode, secCode, parameters));
    }

    public JSONObject getCandles(final String classCode,
                                 final String secCode,
                                 final int interval,
                                 final int maxSize) throws ExecutionException, InterruptedException {
        return (JSONObject) executeMN(new CandlesRequest(classCode, secCode, interval, maxSize));
    }

    public JSONObject getQuoteLevel2(final String classCode,
                                     final String secCode) throws ExecutionException, InterruptedException {
        return (JSONObject) quikConnect.executeMN(
                "getQuoteLevel2", List.of(classCode, secCode),
                requestTimeout.toMillis(), TimeUnit.MILLISECONDS).get("result");
    }

    public JSONArray getQuoteLevel2(final Set<ClassSecCode> classSecCodes) throws ExecutionException, InterruptedException {
        return (JSONArray) executeMN(new BulkQuoteLevel2Request(classSecCodes));
    }
}
