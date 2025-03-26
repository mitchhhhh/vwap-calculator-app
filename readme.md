# VWAP Processing App

Built with Java 17.

## Build 
```
./gradlew assemble
```

## Run tests
```
./gradlew test
```

## Run application
```
./gradlew run
```

## Implementation Summary
Entry point into the processing app is found in `VWAPPricingEngine#transformPriceStream` - creates a `VWAPCalculator per currency pair - which is where most of the logic is implemented.

`VWAPCalculator` uses a sliding window to provide the averages over the past 60 minutes. This is done by creating buckets for each minute, adding prices into the same bucket if they occur within the same minute. As a result this mean that the total number of possible buckets will be _numberOfCurrencyPairs_ * 60. Meaning that our memory usage cannot grow unbounded and cause memory issues 

I've also implemented a small 'market simulator' inside the main class to showcase the functionality, it isn't perfect and is just intended for demonstration purposes (e.g. the price can go negative) 
