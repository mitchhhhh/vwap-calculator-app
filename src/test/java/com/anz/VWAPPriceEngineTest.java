package com.anz;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public class VWAPPriceEngineTest {

    @Test
    public void singleCurrencyPairStream() {
        VWAPPriceEngine vwapPriceEngine = new VWAPPriceEngine();
        Instant now = Instant.now();
        Stream<CurrencyPairPrice> stream = Stream.of(
                new CurrencyPairPrice("AUDUSD", new BigDecimal("1.0"), BigInteger.TEN, now),
                new CurrencyPairPrice("AUDUSD", new BigDecimal("2.0"), BigInteger.TEN, now.plusSeconds(1)),
                new CurrencyPairPrice("AUDUSD", new BigDecimal("3.0"), BigInteger.TEN, now.plusSeconds(2)),
                new CurrencyPairPrice("AUDUSD", new BigDecimal("1.5"), BigInteger.TEN, now.plusSeconds(3))
        );

        Stream<VWAPDataPoint> outputStream = vwapPriceEngine.transformPriceStream(stream);
        List<VWAPDataPoint> expectedOutput = List.of(
                new VWAPDataPoint("AUDUSD", new BigDecimal("1.0000"), now),
                new VWAPDataPoint("AUDUSD", new BigDecimal("1.5000"), now.plusSeconds(1)),
                new VWAPDataPoint("AUDUSD", new BigDecimal("2.0000"), now.plusSeconds(2)),
                new VWAPDataPoint("AUDUSD", new BigDecimal("1.8750"), now.plusSeconds(3))
        );

        assertIterableEquals(outputStream.toList(), expectedOutput);
    }

    @Test
    public void multipleCurrencyPairStream() {
        VWAPPriceEngine vwapPriceEngine = new VWAPPriceEngine();
        Instant now = Instant.now();
        Stream<CurrencyPairPrice> stream = Stream.of(
                new CurrencyPairPrice("AUDUSD", new BigDecimal("1.0"), BigInteger.TEN, now),
                new CurrencyPairPrice("EURUSD", new BigDecimal("2.0"), BigInteger.TEN, now.plusSeconds(1)),
                new CurrencyPairPrice("AUDUSD", new BigDecimal("2.0"), BigInteger.TEN, now.plusSeconds(2)),
                new CurrencyPairPrice("EURUSD", new BigDecimal("1.2"), BigInteger.TEN, now.plusSeconds(3))
        );

        Stream<VWAPDataPoint> outputStream = vwapPriceEngine.transformPriceStream(stream);
        List<VWAPDataPoint> expectedOutput = List.of(
                new VWAPDataPoint("AUDUSD", new BigDecimal("1.0000"), now),
                new VWAPDataPoint("EURUSD", new BigDecimal("2.0000"), now.plusSeconds(1)),
                new VWAPDataPoint("AUDUSD", new BigDecimal("1.5000"), now.plusSeconds(2)),
                new VWAPDataPoint("EURUSD", new BigDecimal("1.6000"), now.plusSeconds(3))
        );

        assertIterableEquals(outputStream.toList(), expectedOutput);
    }

    @Test
    public void invalidCurrencyPair() {
        VWAPPriceEngine vwapPriceEngine = new VWAPPriceEngine();
        Stream<CurrencyPairPrice> stream = Stream.of(
                new CurrencyPairPrice("my ccy", new BigDecimal("1.0"), BigInteger.TEN, Instant.now())
        );

        assertThrowsExactly(IllegalArgumentException.class, () -> vwapPriceEngine.transformPriceStream(stream).toList());
    }

    @Test
    public void invalidVolume() {
        VWAPPriceEngine vwapPriceEngine = new VWAPPriceEngine();
        Stream<CurrencyPairPrice> stream = Stream.of(
                new CurrencyPairPrice("AUDUSD", new BigDecimal("1.0"), BigInteger.ZERO, Instant.now())
        );

        assertThrowsExactly(IllegalArgumentException.class, () -> vwapPriceEngine.transformPriceStream(stream).toList());
    }

}
