package com.algotrading.connectors.quik;

import com.simpleutils.logs.AbstractLogger;
import com.simpleutils.logs.SimpleLogger;
import com.simpleutils.quik.ClassSecCode;
import com.simpleutils.quik.QuikConnect;
import com.simpleutils.quik.QuikListener;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class MarketDataQuikListenerTest {

    private final AbstractLogger logger = new SimpleLogger();

    public static void main(final String[] args) {
        new MarketDataQuikListenerTest().test();
    }

    private void test() {
        logger.withLogLevel(AbstractLogger.TRACE);

        final MarketDataQuikListener marketDataQuikListener = new MarketDataQuikListener();

        marketDataQuikListener.setRequestTimeout(Duration.of(1, ChronoUnit.SECONDS));
        marketDataQuikListener.setCheckConnectedPeriod(Duration.of(1, ChronoUnit.SECONDS));
        marketDataQuikListener.setPauseAfterException(Duration.of(15, ChronoUnit.SECONDS));
        marketDataQuikListener.setSubscriptionPeriod(Duration.of(10, ChronoUnit.SECONDS));
        marketDataQuikListener.setOnlineDuration(Duration.of(15, ChronoUnit.SECONDS));

        marketDataQuikListener.setLogger(logger);
        marketDataQuikListener.setLogPrefix(MarketDataQuikListenerTest.class.getSimpleName() + ": ");

        marketDataQuikListener.addCallbackSubscription("OnTrade", "*");
        marketDataQuikListener.addCallbackSubscription("OnOrder", "*");
        marketDataQuikListener.addCallbackSubscription("OnAllTrade", "*");
        marketDataQuikListener.addCallbackSubscription("OnQuote", "*");

        marketDataQuikListener.addSecurityParameters(ClassSecCode.of("TQBR", "SBER"), List.of("LAST", "BID", "OFFER"));
        marketDataQuikListener.addSecurityParameters(ClassSecCode.of("TQBR", "GAZP"), new String[]{"LAST", "BID", "OFFER"});
        marketDataQuikListener.addSecurityParameters(ClassSecCode.of("TQBR", "LKOH"), List.of("LAST", "BID", "OFFER"));
        marketDataQuikListener.addSecurityParameter(ClassSecCode.of("CETS", "USD000UTSTOM"), "LAST");

        marketDataQuikListener.addSecurityCandles(ClassSecCode.of("TQBR", "SBER"), List.of(1, 5, 10, 1440));
        marketDataQuikListener.addSecurityCandles(ClassSecCode.of("TQBR", "GAZP"), new int[]{5, 15, 60});
        marketDataQuikListener.addSecurityCandles(ClassSecCode.of("TQBR", "LKOH"), 30);
        marketDataQuikListener.addSecurityCandles(ClassSecCode.of("CETS", "USD000UTSTOM"), 60);

        marketDataQuikListener.addLevel2Quotes(ClassSecCode.of("TQBR", "SBER"));
        marketDataQuikListener.addLevel2Quotes(ClassSecCode.of("TQBR", "GAZP"));
        marketDataQuikListener.addLevel2Quotes(ClassSecCode.of("TQBR", "LKOH"));
        marketDataQuikListener.addLevel2Quotes(ClassSecCode.of("CETS", "USD000UTSTOM"));

        final QuikConnect quikConnect = new QuikConnect(
                "localhost",
                10001,
                10002,
                MarketDataQuikListenerTest.class.getSimpleName(),
                marketDataQuikListener);
        marketDataQuikListener.setQuikConnect(quikConnect);

        quikConnect.start();

        final ZonedDateTime stoppingTime = ZonedDateTime.now().plus(5, ChronoUnit.MINUTES);
        while (ZonedDateTime.now().isBefore(stoppingTime) && !Thread.currentThread().isInterrupted()) {
            processRunnables(marketDataQuikListener);
            marketDataQuikListener.ensureConnection();
            marketDataQuikListener.ensureSubscription();
            if (ZonedDateTime.now().getSecond() == 0) {
                logger.debug("Force subscription");
                marketDataQuikListener.subscribe();
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                //noinspection BusyWait
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        quikConnect.shutdown();
        processRunnables(marketDataQuikListener);
    }

    private void processRunnables(final QuikListener quikListener) {
        Runnable runnable;
        while ((runnable = quikListener.poll()) != null) {
            try {
                runnable.run();
            } catch (final Exception e) {
                logger.log(AbstractLogger.ERROR, "Cannot execute a runnable from "
                        + quikListener.getClass().getSimpleName(), e);
            }
        }
    }
}
