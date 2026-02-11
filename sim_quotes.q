/ Simulate bid/ask prices and sizes from prices_table (one quote per bar)

\l /Users/zacharydugan/q/cz/cz/big_sim_db/db

/ Bring partitioned prices_table into memory
prices_flat: select from prices_table;

n: count prices_flat;

/ Random quote times within 1 minute of each bar_time
/ Randomize the seconds field (0–59) while keeping the hour and minute from bar_time
baseStr: string each prices_flat`bar_time;         / "hh:mm:ss" per row
randSecs: n?60;                                    / 0..59 per row
secsStr: { -2#("0", string x) } each randSecs;     / "00".."59"
quoteStr: { (-2 _ x), y }'[baseStr; secsStr];      / replace last 2 chars with new seconds
quote_time: `$ quoteStr;                           / parse back to time/second

/ Mid price is the bar price; bid/ask are a tight spread around it
mid: prices_flat[`price];
/ spread: 0.002 * mid;               / 0.2% spread
lo: 0.002;
hi: 0.004;
spread: lo + (hi - lo) * n?1f;
bid: mid - 0.5 * spread;
ask: mid + 0.5 * spread;

/ Sizes between 100 and 1000 in steps of 100, cycling deterministically
randLots: 1 + mod[til n; 10];      / 1..10 repeated
bidSize: 100 * randLots;
randLots2: 1 + mod[3 + til n; 10]; / shifted pattern for ask sizes, same length as n
askSize: 100 * randLots2;

/ Build quotes table by adding columns onto prices_flat
quotes_table: update
  quote_time: quote_time,
  bid:        bid,
  ask:        ask,
  bidSize:    bidSize,
  askSize:    askSize
  from prices_flat;

show "quotes_table sample (first 10 rows):";
show 10#quotes_table;
show "total quotes count:";
show count quotes_table;

show "adding Slippage"
// Add MPPs + Slippage
quotes_table: update MPP: (bid + ask)%2. from quotes_table;
quotes_table: update WMPP: ( (bid*askSize) + (ask * bidSize) )%(bidSize + askSize) from quotes_table;
quotes_table: update Slippage: price - MPP from quotes_table;
quotes_table: update WSlippage: price - WMPP from quotes_table;

select date, bar_time, sym, price, MPP, WMPP, WSlippage from quotes_table //where WSlippage > 0.05
