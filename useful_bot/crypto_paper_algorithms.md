# Crypto Paper Bot Algorithms

This document explains the six trading algorithms used by [`crypto_paper_bot.py`](/d:/Chrome/911/LystTgFirefox/crypto_paper_bot.py), what each one is trying to capture, and what technical conditions must be true before a paper trade is opened.

## Shared Runtime Context

All six algorithms operate on Binance spot `USDT` pairs using `1h` candles. The bot fetches `300` candles per symbol so slow indicators such as `EMA 200` have enough warmup history. Signals are evaluated only on the latest fully closed candle, not on the currently forming candle.

All trades are long-only paper trades. Each trade opens with a synthetic notional of `$1,000`, a fixed take-profit at `+15%`, and a fixed stop-loss at `-5%`. Only one open trade per coin is allowed across all algorithms, so if one algorithm opens `BTC/USDT`, the others are blocked from entering `BTC/USDT` until that trade closes.

The indicators are calculated with `pandas_ta`. The original algorithms are `Algo 1` to `Algo 3`. The improved regime-aware variants are `Algo 4` to `Algo 6`.

## Algo 1: Squeeze Breakout

### Core idea

This is a volatility-compression breakout strategy. It tries to enter when price has been trading in an unusually tight range and then expands upward with strong participation.

### What it uses

- Bollinger Bands `(20, 2)`
- Bollinger Bandwidth
- Volume
- `20`-period volume SMA

### Entry conditions

The bot opens a trade when all of the following are true on the latest closed candle:

1. The current Bollinger bandwidth is lower than every one of the previous `14` closed candles.
2. The current close is above the upper Bollinger Band.
3. The current volume is greater than `2x` the `20`-period volume SMA.

### Why it exists

The logic assumes compressed markets often precede expansion. A close above the upper band suggests expansion has started, and the volume rule tries to filter out weak breakouts that happen without real participation.

### Main weakness

It is a rigid pattern. In noisy markets, price can briefly poke above the upper band and still fail immediately. The rule also has no market-regime filter, so it can fire in conditions where breakouts historically perform poorly.

## Algo 2: Golden Dip

### Core idea

This is a trend-pullback strategy. It looks for a strong bullish trend, then tries to buy a dip into the medium-term trend line while short-term momentum is oversold.

### What it uses

- `EMA 50`
- `EMA 200`
- RSI `(14)`
- Candle low

### Entry conditions

The bot opens a trade when all of the following are true on the latest closed candle:

1. `EMA 50 > EMA 200`
2. The current candle low is less than or equal to `EMA 50`
3. RSI `(14)` is below `35`

### Why it exists

The `EMA 50 > EMA 200` condition defines a bullish trend. A touch of `EMA 50` represents a pullback into support, and low RSI tries to identify that the pullback has reached a stretched condition rather than buying at a local high.

### Main weakness

It can buy dips that are not healthy pullbacks but the start of a deeper breakdown. It also does not require any recovery confirmation after the dip, so it can enter too early.

## Algo 3: Reversal

### Core idea

This is an early reversal strategy. It tries to buy when downside momentum is fading and both trend-following and momentum indicators start turning upward from below neutral territory.

### What it uses

- MACD line
- MACD signal line
- RSI `(14)`

### Entry conditions

The bot opens a trade when all of the following are true:

1. On the previous candle, `MACD line <= MACD signal`
2. On the latest candle, `MACD line > MACD signal`
3. Both the latest MACD line and signal line are still below `0`
4. On the previous candle, `RSI <= 50`
5. On the latest candle, `RSI > 50`

### Why it exists

The algorithm is trying to catch a change from bearish momentum to bullish momentum before the move is fully mature. Requiring both MACD values below zero means it is still looking for reversals that start from a weak backdrop rather than buying already-extended uptrends.

### Main weakness

This type of signal is vulnerable to whipsaws. MACD and RSI can cross repeatedly in sideways or weakly bearish markets, which creates many false starts.

## Algo 4: Improved Squeeze Breakout

### Core idea

This is the improved version of Algo 1. It still targets volatility compression followed by upside expansion, but it only trades when the broader regime looks favorable and the breakout has stronger confirmation.

### What it adds on top of Algo 1

- `ADX 14`
- `EMA 50`
- `EMA 200`
- `EMA 50` slope over `5` bars
- `24`-hour average dollar volume
- `100`-bar percentile rank of Bollinger bandwidth
- Previous candle high as follow-through confirmation

### Regime filter

The improved breakout only becomes eligible when the market is classified as a supportive trend regime:

- `EMA 50 > EMA 200`
- `ADX 14 >= 20`
- `EMA 50` slope over `5` bars is positive
- Bollinger bandwidth percentile rank over the trailing `100` bars is at or below `15%`

### Entry conditions

The bot opens a trade when all of the following are true on the latest closed candle:

1. The trend regime above is active.
2. The close is above the upper Bollinger Band.
3. The close is above the previous candle high.
4. Volume is at least `1.5x` the `20`-period volume SMA.

### Why it is better than Algo 1

Instead of using a simple "lowest bandwidth in 14 candles" rule, it measures how unusually compressed the market is relative to a longer context. It also requires a trend regime and breakout follow-through, which reduces trades in weak or choppy environments.

## Algo 5: Improved Golden Dip

### Core idea

This is the improved version of Algo 2. It still buys pullbacks in an uptrend, but it waits for evidence that price is recovering after the dip instead of buying immediately on touch.

### What it adds on top of Algo 2

- `EMA 20`
- `ADX 14`
- `EMA 50` slope over `5` bars
- Previous candle high reclaim

### Regime filter

The pullback is only eligible when the market is in a strong uptrend:

- `EMA 50 > EMA 200`
- `ADX 14 >= 20`
- `EMA 50` slope over `5` bars is positive

### Entry conditions

The bot opens a trade when all of the following are true:

1. The strong-uptrend regime above is active.
2. The candle low touches or dips below `EMA 50`.
3. The candle closes at or above `EMA 20`.
4. The candle closes above the previous candle high.
5. RSI `(14)` is at or below `40`.

### Why it is better than Algo 2

The extra confirmation tries to separate a healthy pullback from a failing trend. A reclaim above `EMA 20` and above the previous candle high means buyers actually stepped back in before entry.

## Algo 6: Improved Reversal

### Core idea

This is the improved version of Algo 3. It still looks for early reversals, but it only trades liquid, range-like conditions and avoids strong downtrends where reversal signals fail more often.

### What it adds on top of Algo 3

- `ADX 14`
- `24`-hour average dollar volume
- MACD histogram slope
- Previous candle high reclaim
- Downtrend rejection

### Regime filter

The reversal is only eligible when all of the following are true:

- The symbol is liquid, with `24`-hour average dollar volume of at least `$2,000,000`
- `ADX 14 <= 15`, which is treated as range-bound rather than strongly trending
- The symbol is not in a strong downtrend, defined as `EMA 50 < EMA 200` with `ADX 14 >= 20`

### Entry conditions

The bot opens a trade when all of the following are true:

1. MACD crosses up above its signal line on the latest candle.
2. Both MACD line and signal line are still below `0`.
3. MACD histogram slope is non-negative.
4. RSI crosses above `50`.
5. The latest close is above the previous candle high.

### Why it is better than Algo 3

It is much more selective. The original reversal logic can trigger in low-quality downtrends and illiquid symbols. The improved version tries to keep reversals only in liquid, non-trending conditions where the market is statistically more likely to mean-revert instead of continuing lower.

## Practical Interpretation

The six algorithms fall into three concept pairs:

- `Algo 1` and `Algo 4`: breakout from compression
- `Algo 2` and `Algo 5`: pullback inside trend
- `Algo 3` and `Algo 6`: early reversal

The original algorithms are intentionally simple and broad. They can capture more setups, but they also admit more false positives. The improved algorithms trade the same ideas with additional regime and confirmation filters, so they should usually be more selective and more conservative.

## Current Risk Model

Even the improved algorithms still inherit the current live paper-trading risk model:

- fixed `$1,000` synthetic size
- fixed `+15%` take-profit
- fixed `-5%` stop-loss
- one open trade per symbol across all algorithms
- same-candle TP/SL conflict is resolved as `LOSS`

That means the signal quality is more advanced in `Algo 4` to `Algo 6`, but the exit logic is still the same fixed paper-trading model used by the live bot.

## Maintenance Rule

If any crypto paper-trading algorithm is changed in the future, this file must be updated in the same change so the documented behavior matches the live bot exactly.
