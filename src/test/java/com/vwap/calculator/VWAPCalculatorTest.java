package com.vwap.calculator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;

public class VWAPCalculatorTest {

    @Test
    public void testVariablePrice() {
        VWAPCalculator calculator = new VWAPCalculator();
        BigInteger fixedVolume = BigInteger.TEN;
        String currencyPair = "AUDUSD";

        CurrencyPairPrice price1 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(1.0), fixedVolume, Instant.now());
        CurrencyPairPrice price2 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(2.0), fixedVolume, price1.timestamp().plusSeconds(65));
        CurrencyPairPrice price3 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(3.0), fixedVolume, price2.timestamp().plusSeconds(65));

        VWAPDataPoint vwap1 = calculator.process(price1);
        VWAPDataPoint vwap2 = calculator.process(price2);
        VWAPDataPoint vwap3 = calculator.process(price3);

        assertEquals(vwap1, new VWAPDataPoint(currencyPair, new BigDecimal("1.0000"), price1.timestamp()));
        assertEquals(vwap2, new VWAPDataPoint(currencyPair, new BigDecimal("1.5000"), price2.timestamp()));
        assertEquals(vwap3, new VWAPDataPoint(currencyPair, new BigDecimal("2.0000"), price3.timestamp()));
    }

    @Test
    public void testAddToBucket() {
        VWAPCalculator calculator = new VWAPCalculator();
        BigInteger fixedVolume = BigInteger.TEN;
        String currencyPair = "AUDUSD";

        CurrencyPairPrice price1 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(1.0), fixedVolume, Instant.now());
        CurrencyPairPrice price2 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(2.0), fixedVolume, price1.timestamp());
        CurrencyPairPrice price3 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(3.0), fixedVolume, price2.timestamp().plusSeconds(65));

        VWAPDataPoint vwap1 = calculator.process(price1);
        VWAPDataPoint vwap2 = calculator.process(price2);
        VWAPDataPoint vwap3 = calculator.process(price3);

        assertEquals(vwap1, new VWAPDataPoint(currencyPair, new BigDecimal("1.0000"), price1.timestamp()));
        assertEquals(vwap2, new VWAPDataPoint(currencyPair, new BigDecimal("1.5000"), price2.timestamp()));
        assertEquals(vwap3, new VWAPDataPoint(currencyPair, new BigDecimal("2.0000"), price3.timestamp()));
    }

    @Test
    public void testVariableVolume() {
        VWAPCalculator calculator = new VWAPCalculator();
        BigDecimal fixedPrice = BigDecimal.valueOf(0.63);
        String currencyPair = "AUDUSD";

        CurrencyPairPrice price1 = new CurrencyPairPrice(currencyPair, fixedPrice, BigInteger.valueOf(73), Instant.now());
        CurrencyPairPrice price2 = new CurrencyPairPrice(currencyPair, fixedPrice, BigInteger.valueOf(48), Instant.now());
        CurrencyPairPrice price3 = new CurrencyPairPrice(currencyPair, fixedPrice, BigInteger.valueOf(11), Instant.now());

        calculator.process(price1);
        VWAPDataPoint vwap2 = calculator.process(price2);
        VWAPDataPoint vwap3 = calculator.process(price3);

        // no need to assert on vwap1 in any of the other tests as its functionally the same as the assertion in the first test
        assertEquals(vwap2, new VWAPDataPoint(currencyPair, fixedPrice, price2.timestamp()));
        assertEquals(vwap3, new VWAPDataPoint(currencyPair, fixedPrice, price3.timestamp()));
    }

    @Test
    public void testVariablePriceAndVolume() {
        VWAPCalculator calculator = new VWAPCalculator();
        String currencyPair = "AUDUSD";

        CurrencyPairPrice price1 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(1.0), BigInteger.valueOf(30), Instant.now());
        CurrencyPairPrice price2 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(2.0), BigInteger.valueOf(20), Instant.now());
        CurrencyPairPrice price3 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(3.0), BigInteger.valueOf(50), Instant.now());

        calculator.process(price1);
        VWAPDataPoint vwap2 = calculator.process(price2);
        VWAPDataPoint vwap3 = calculator.process(price3);

        // ((1 * 30) + (2 * 20)) / (30 + 20) = 70 / 50 = 1.4
        assertEquals(vwap2, new VWAPDataPoint(currencyPair, new BigDecimal("1.40"), price2.timestamp()));

        // (70 + (3 * 50)) / (50 + 50) = 220 / 100 = 2.2
        assertEquals(vwap3, new VWAPDataPoint(currencyPair, new BigDecimal("2.20"), price3.timestamp()));
    }

    @Test
    public void testOneHourWindow() {
        VWAPCalculator calculator = new VWAPCalculator();
        Instant now = Instant.now();
        BigInteger fixedVolume = BigInteger.TEN;
        String currencyPair = "AUDUSD";

        Instant oneHourAgo = now.minus(Duration.ofHours(1));
        Instant halfAnHourAgo = now.minus(Duration.ofMinutes(30));

        CurrencyPairPrice price1 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(1.0), fixedVolume, oneHourAgo);
        CurrencyPairPrice price2 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(2.0), fixedVolume, halfAnHourAgo);
        CurrencyPairPrice price3 = new CurrencyPairPrice(currencyPair, BigDecimal.valueOf(3.0), fixedVolume, now);

        calculator.process(price1);
        VWAPDataPoint vwap1 = calculator.process(price2);
        VWAPDataPoint vwap2 = calculator.process(price3);

        assertEquals(vwap1, new VWAPDataPoint(currencyPair, new BigDecimal("1.50"), price2.timestamp()));
        assertEquals(vwap2, new VWAPDataPoint(currencyPair, new BigDecimal("2.50"), price3.timestamp()));
    }
}
