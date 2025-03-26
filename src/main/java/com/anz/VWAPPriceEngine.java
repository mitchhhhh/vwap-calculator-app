package com.anz;

import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Currency;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class VWAPPriceEngine {

    // Since there's only a finite number of currencies and not all possible currency pairs are traded,
    // the overall memory footprint should be bounded to a reasonable number.
    // But more generally, the space complexity is a function of the number of currency pairs

    // shouldn't need concurrent hashmap as the stream is sequential
    private final Map<String, VWAPCalculator> vwapMap = new HashMap<>();
    private static final int PRICE_SCALE = 4;

    public Stream<VWAPDataPoint> transformPriceStream(Stream<CurrencyPairPrice> inputStream) {
        // convert the stream to a sequential stream to ensure prices for currency pairs are processed in order
        // IRL this would be some sort of message queue, and we could guarantee sequential processing through
        // appropriate partitioning of the queue (i.e. partition on currency pair).
        return inputStream.sequential().map(this::transformPrice);
    }

    private VWAPDataPoint transformPrice(CurrencyPairPrice currencyPairPrice) {
        CurrencyPairPrice sanitisedInput = sanitiseInput(currencyPairPrice);
        VWAPCalculator calculator = vwapMap.computeIfAbsent(
                currencyPairPrice.currencyPair(),
                k -> new VWAPCalculator()
        );

        return calculator.process(sanitisedInput);
    }

    // probably unnecessary, depends on where this process would sit in the pipeline
    private CurrencyPairPrice sanitiseInput(CurrencyPairPrice currencyPairPrice) {
        try {
            Currency.getInstance(currencyPairPrice.currencyPair().substring(0, 3));
            Currency.getInstance(currencyPairPrice.currencyPair().substring(3));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid currency pair: " + currencyPairPrice.currencyPair());
        }

        if (currencyPairPrice.volume().compareTo(BigInteger.ZERO) <= 0)
            throw new IllegalArgumentException("Volume must be greater than zero: volume=" + currencyPairPrice.volume());

        return new CurrencyPairPrice(
                currencyPairPrice.currencyPair(),
                currencyPairPrice.price().setScale(PRICE_SCALE, RoundingMode.HALF_EVEN),
                currencyPairPrice.volume(),
                currencyPairPrice.timestamp()
        );
    }
}
