/ UNIFIED KDB+ PIPELINE: Simulation -> Returns -> Medians -> EMA
/ Windows Compatible Version
show "--- Initializing Unified Pipeline ---";

/ --- SECTION 1: Environment & Libraries ---
\l utility.q

/ Define Paths using strings to handle Windows special characters (桌面)
dbPathStr: "/Users/zacharydugan/q/simulated_data/db";
// \l /Users/zacharydugan/q/simulated_data/db

dbPath: hsym `$dbPathStr;

startDate: 2023.01.02;
endDate: 2025.01.02;
allDays: startDate + til 1 + endDate - startDate;
tradingDays: allDays where (mod[allDays; 7]) < 5; 
marketMinutes: 390;
barTimes: 09:30:00 + 00:01 * til marketMinutes; 
syms: `IBM`AAPL`TSLA; 
basePrices: 185.0 185.0 250.0; 

/ --- SECTION 2: Step 1 - Simulation Logic ----------------------------------------------------------------
show "Step 1: Running Price Simulation...";
simulate_day_one_sym: {[dt; sym; startP]
    n: marketMinutes;
    returns: 0.001 * sums (n?1.0) - 0.5; 
    prices: startP * 1 + returns;
    ([] date: dt; bar_time: barTimes; sym: n#sym; price: prices)
 };

construct_table_one_day: {[dt; openP]
    t: raze { [dt; sym; op] simulate_day_one_sym[dt; sym; op] }[dt] ' [syms; openP]; 
    `date`bar_time xasc t
 };

//If dbPath doesn't exist then make it.
if[()~key dbPath; .[system;("mkdir \"",dbPathStr,"\"");{}]];

openP: basePrices; 
i: 0;
while[i < count tradingDays;
    dt: tradingDays[i];
    day_table: construct_table_one_day[dt; openP];
    prices_table:: select bar_time, sym, price from day_table; 
    .Q.dpft[dbPath; dt; `sym; `prices_table]; 
    openP: exec last price by sym from prices_table; 
    if[0 = i mod 50; show "Partition saved: ", string dt];
    i+: 1
 ];
/ __________________________________________________________________________________________________________
/ --- SECTION 3: Step 2 - Returns Calculation --- Calculates and saves ret_table
show "Step 2: Calculating Returns...";
\l /Users/zacharydugan/q/simulated_data/db
p: `date`sym`bar_time xasc select from prices_table where date within (startDate; endDate); 
returns_table: update return: (price - prev price) % prev price by date, sym from p;

{ [dt]
    ret_table:: select date, bar_time, sym, return from returns_table where date = dt;
    .Q.dpft[dbPath; dt; `sym; `ret_table]
 } each tradingDays;
/ __________________________________________________________________________________________________________


/ __________________________________________________________________________________________________________
/ --- SECTION 4: Step 3 - Moving Medians --- Creates and saves moving_median_table
show "Step 3: Calculating Moving Medians...";
\l /Users/zacharydugan/q/simulated_data/db

/ Load prices and calculate moving median across time (window 7)
/ We group by sym and bar_time to look at the median of that specific time-slice across dates
// median_data: select from prices_table;
median_data: select from ret_table;
median_data: update moving_median: return_moving_window_op[7; med; return] by sym, bar_time from median_data;

{ [dt]
    moving_median_table:: select date, bar_time, sym, moving_median from median_data where date = dt;
    .Q.dpft[dbPath; dt; `sym; `moving_median_table]
 } each tradingDays;
/ __________________________________________________________________________________________________________


/ __________________________________________________________________________________________________________
/ --- SECTION 5: Step 4 - Exponential Medians (EMA) --- Saves to exp_median_table
show "Step 4: Applying EMA to Medians...";
\l /Users/zacharydugan/q/simulated_data/db

/ Use the moving_median_table as input for the EMA
// ema_data: select from moving_median_table;
// ema_data: update exp_median: (6#0n), ema[0.333; 6_moving_median] by sym, bar_time from ema_data;

ema_data: select from moving_median_table where date > 2023.01.10;  / 7 day moving median
/ Apply EMA (alpha 0.333) to the moving median values, ignoring the first 6 nulls


return_moving_median_array:{ [input_sym, input_bar_time]
    / Just for checking
    / moving_median_array: select date, sym, bar_time, moving_median from ema_data where (sym=`AAPL) and (bar_time=09:31:00);
    / moving_median_array: exec moving_median from ema_data where (sym=`AAPL) and (bar_time=09:31:00)
    moving_median_array: exec moving_median from ema_data where (sym=input_sym) and (bar_time=input_bar_time)
}

moving_median_array: return_moving_median_array[ `AAPL, 09:31:00]
input_array: ema[0.333; moving_median_array]

cond1: where (  (ema_data`bar_time)=indiv_time)  ;
cond2: where (  ema_data`sym)=indiv_sym;
inds: cond1 inter cond2;

amended_col: @[ema_data`exp_median; inds; :; "f"$input_array];
ema_data:: update exp_median: amended_col from exp_median;


ema_data: update exp_median: (6#0n), ema[0.333; moving_median] by sym, bar_time from ema_data;
/ ema_data: update exp_median: (6#0n), ema[0.333; moving_median] by sym, bar_time from ema_data;
/ ema_data: update exp_median: (6#0n), ema[0.333; moving_median] from ema_data where (sym=sym) and (bar_time=bar_time);
/ ema_data: update exp_median: ema[0.333; moving_median] from ema_data where (sym=sym) and (bar_time=bar_time)

show "Showing ema results"
select ema[0.333; 6_moving_median] by sym, bar_time from ema_data;
select ema[0.333; 10_moving_median] by sym, bar_time from ema_data;
show 20#(ema_data`exp_median)
show -20#(ema_data`exp_median)
show 20#(ema_data)
show -20#(ema_data)

{ [dt]
    exp_median_table:: select date, bar_time, sym, moving_median, exp_median from ema_data where date = dt;
    .Q.dpft[dbPath; dt; `sym; `exp_median_table]
 } each tradingDays;
/ __________________________________________________________________________________________________________


/ __________________________________________________________________________________________________________
 / --- SECTION 5: Step 4 - Exponential Medians (Nudge Logic) ---
show "Step 5: Applying Exponential Nudge Median...";
\l /Users/zacharydugan/q/simulated_data/db

/ Define your custom nudge parameters
alpha: 0.05;
exp_step: {[a; m; p] $[p > m; m + a; p < m; m - a; m]};

alpha: 0.05;
exp_step: {  [a; m; p] 
    $[p > m; 
    m + a; 
    p < m; 
    m - a; 
    m]  };

/ Load temporary table for calculation
temp_table: select date, sym, bar_time, price from prices_table;

/ Calculate Exponential Median using scan iterator (\)
/ It nudges the median toward the price based on alpha
median_results: update exp_med: exp_step[alpha;;]\[first price; price] by date, sym from temp_table;

/ Show last 21 rows for verification
show "Last 21 rows of results:";
show -21#median_results;
/ Persist the final analytics table
{ [dt]
    exp_median_nudge_table:: select date, bar_time, sym, price, exp_med from median_results where date = dt;
    .Q.dpft[dbPath; dt; `sym; `exp_median_nudge_table]
 } each tradingDays;
/ __________________________________________________________________________________________________________

/ __________________________________________________________________________________________________________
/ Calculate and show correlation

\l /Users/zacharydugan/q/simulated_data/db

/ last_21: -21#median_results;
/ result: exec price cor exp_med from last_21;
/ show "Correlation of last 21 rows:";
/ show result;

/ --- SECTION 6: Step 5 - Alternative Exponential Medians (EMA) ---

/ --- 1. Set your filters ---
sym_filter: `AAPL;
/ Use a date from your simulation (e.g., 10 days after start)
date_filter: startDate + 10; 

/ --- 2. Extract columns as flat numeric lists using 'exec' ---
/ We pull both price and exp_med from the same table to ensure they align
/ p: exec exp_med from exp_median_table where sym=sym_filter, date > date_filter;
p: select exp_median from exp_median_table where sym=sym_filter, date > date_filter;
p2: exec exp_median from p
m: exec exp_med from exp_median_nudge_table where sym=sym_filter, date > date_filter;

/ --- 3. Run the correlation ---
/ This will return a single float between -1 and 1
result: p2 cor m;

show "Correlation for ", (string sym_filter), " since ", (string date_filter), ":";
show result;

// / --- SECTION 6: Final Correlation Analysis ---
show "Step 5: Generating Correlation Report...";

/ Calculate for all symbols in the last 21 minutes of the latest date
latest_dt: last tradingDays;
latest_data: select from exp_median_table where date = latest_dt;

/ Generate a table showing correlation for all symbols
final_report: select 
    last_21_cor: (-21#price) cor (-21#exp_med),
    full_day_cor: price cor exp_med 
    by sym from latest_data;

show "Final Correlation Report for ", string latest_dt;
show final_report;

show "--- Pipeline Completed Successfully ---";
