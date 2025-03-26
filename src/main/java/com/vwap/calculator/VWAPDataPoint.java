package com.vwap.calculator;

import java.math.BigDecimal;
import java.time.Instant;

public record VWAPDataPoint(String currencyPair, BigDecimal vwap, Instant timestamp) {}
