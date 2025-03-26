package com.anz;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    private final static MarketSimulator simulator = new MarketSimulator();
    public static void main(String[] args) {
        VWAPPriceEngine engine = new VWAPPriceEngine();
        Stream<CurrencyPairPrice> stream = Stream.generate(
                () -> {
                    try {
                        // this is just to slow down the output printed to the screen so that its readable
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return simulator.nextCurrencyPairPrice();
                }
        ).peek(price ->
                System.out.println("Currency Pair " + price.currencyPair() + ": price=" + price.price() +
                        ", volume=" + price.volume() + " at " + price.timestamp())
        );

        engine.transformPriceStream(stream).forEach(vwap ->
                System.out.println("Current VWAP for " + vwap.currencyPair() + " is " + vwap.vwap() + " at " + vwap.timestamp())
        );
    }

    // very simple mock class to showcase the application as a running stream
    private static class MarketSimulator {
        private final List<String> currencyPairs = List.of("AUDUSD", "EURUSD", "GBPUSD");
        private final Map<String, CurrencyPairPrice> m = currencyPairs.stream().collect(
                Collectors.toMap(k -> k, k -> new CurrencyPairPrice(k, BigDecimal.ONE, BigInteger.TEN, Instant.now()))
        );
        private final Random random = new Random();

        CurrencyPairPrice nextCurrencyPairPrice() {
            String currencyPair =  currencyPairs.get(random.nextInt(currencyPairs.size()));
            CurrencyPairPrice currentPrice = m.get(currencyPair);
            BigDecimal priceMovement = new BigDecimal(random.nextInt(21) - 10)
                    .setScale(4, RoundingMode.HALF_EVEN)
                    .divide(BigDecimal.valueOf(100), RoundingMode.HALF_EVEN); // can move by up to 0.01 up or down

            CurrencyPairPrice nextPrice = new CurrencyPairPrice(
                    currencyPair,
                    currentPrice.price().add(priceMovement),
                    BigInteger.valueOf(random.nextInt(1, 101)),
                    Instant.now()
            );

            m.put(currencyPair, nextPrice);
            return nextPrice;
        }
    }
}
