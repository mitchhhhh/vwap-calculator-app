package com.vwap.calculator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

public record CurrencyPairPrice(String currencyPair, BigDecimal price, BigInteger volume, Instant timestamp) {
    BigDecimal tradeValue() {
        return price.multiply(new BigDecimal(volume));
    }
}
