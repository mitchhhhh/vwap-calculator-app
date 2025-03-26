package com.vwap.calculator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;

public class VWAPCalculator {
    // Sliding window is implemented as (up to) 60 1 minute buckets. I've added buckets to ensure memory
    // usage can't grow unbounded even if many prices are received in a short time frame
    private final LinkedList<MinuteBucket> slidingWindow = new LinkedList<>();
    private final static Duration ONE_HOUR = Duration.of(1, ChronoUnit.HOURS);
    private final static int VWAP_SCALE = 4;

    private BigDecimal tradeValueInWindow = BigDecimal.ZERO;
    private BigInteger volumeInWindow = BigInteger.ZERO;

    public VWAPDataPoint process(CurrencyPairPrice currencyPairPrice) {
        Instant bucketKey = currencyPairPrice.timestamp().truncatedTo(ChronoUnit.MINUTES);

        if (slidingWindow.isEmpty()) {
            addNewBucket(bucketKey, currencyPairPrice);
        } else if (slidingWindow.getFirst().bucketKey.equals(bucketKey)) {
            updateBucket(slidingWindow.getFirst(), currencyPairPrice);
        } else {
            removeBucketsOutsideWindow(bucketKey);
            addNewBucket(bucketKey, currencyPairPrice);
        }

        return new VWAPDataPoint(currencyPairPrice.currencyPair(), getCurrentVWAP(), currencyPairPrice.timestamp());
    }

    private BigDecimal getCurrentVWAP() {
        return tradeValueInWindow.divide(new BigDecimal(volumeInWindow), VWAP_SCALE, RoundingMode.HALF_EVEN);
    }

    private void addNewBucket(Instant bucketKey, CurrencyPairPrice currencyPairPrice) {
        MinuteBucket bucket = new MinuteBucket(bucketKey, currencyPairPrice.tradeValue(), currencyPairPrice.volume());
        slidingWindow.addFirst(bucket);
        addToWindowTotals(currencyPairPrice);
    }

    private void updateBucket(MinuteBucket bucket, CurrencyPairPrice currencyPairPrice) {
        bucket.tradeValue = bucket.tradeValue.add(currencyPairPrice.tradeValue());
        bucket.volume = bucket.volume.add(currencyPairPrice.volume());
        addToWindowTotals(currencyPairPrice);
    }

    private void addToWindowTotals(CurrencyPairPrice currencyPairPrice) {
        tradeValueInWindow = tradeValueInWindow.add(currencyPairPrice.tradeValue());
        volumeInWindow = volumeInWindow.add(currencyPairPrice.volume());
    }

    private void removeBucketsOutsideWindow(Instant bucketKey) {
        while (!slidingWindow.isEmpty() && slidingWindow.getLast().bucketKey.plus(ONE_HOUR).compareTo(bucketKey) <= 0) {
            MinuteBucket removedElement = slidingWindow.removeLast();
            tradeValueInWindow = tradeValueInWindow.subtract(removedElement.tradeValue);
            volumeInWindow = volumeInWindow.subtract(removedElement.volume);
        }
    }

    private static class MinuteBucket {
        private final Instant bucketKey;
        private BigDecimal tradeValue;
        private BigInteger volume;

        MinuteBucket(Instant bucketKey, BigDecimal tradeValue, BigInteger volume) {
            this.bucketKey = bucketKey;
            this.tradeValue = tradeValue;
            this.volume = volume;
        }
    }
}
