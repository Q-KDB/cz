/ UNIFIED KDB+ PIPELINE: Simulation -> Returns -> Medians -> EMA
/ Windows Compatible Version
show "--- Initializing Unified Pipeline ---";

/ --- SECTION 1: Environment & Libraries ---
\l utility.q

/ Define Paths using strings to handle Windows special characters (桌面)
dbPathStr: "/Users/zacharydugan/q/big_sim_db";
dbPathStr_am: "/Users/zacharydugan/q/big_sim_db/am";
dbPathStr_nz: "/Users/zacharydugan/q/big_sim_db/nz";
`:/Users/zacharydugan/q/big_sim_db/db/par.txt 0: ("/Users/zacharydugan/q/big_sim_db/am"; "/Users/zacharydugan/q/big_sim_db/nz")


dbPath: hsym `$dbPathStr;
dbPath_am: hsym `$dbPathStr_am;
dbPath_nz: hsym `$dbPathStr_nz;

startDate: 2023.01.02;
endDate: 2025.01.02;
allDays: startDate + til 1 + endDate - startDate;
tradingDays: allDays where (mod[allDays; 7]) < 5; 
marketMinutes: 390;
barTimes: 09:30:00 + 00:01 * til marketMinutes; 
/ syms: `IBM`AAPL`TSLA`MSFT`AMZN`META`WMT`KO`DIS`NVDA; 
syms: `ibm`appl`tsla`msft`amzn`meta`wmt`ko`dis`nvda;
basePrices: 25. 50. 75. 100. 125. 150. 175. 200. 225. 250.; 

/ openP: basePrices; 
openP: syms!basePrices; 
show "openP 1";
show openP;
// show "exiting here"
// \

extr:{[t;r] select from t where (`$1#'string sym) within r}


/ --- SECTION 2: Step 1 - Simulation Logic ----------------------------------------------------------------
show "Step 1: Running Price Simulation...";
simulate_day_one_sym: {[dt; sym; startP]
    / if[sym=`wmt; show "wmt OpenP ", string startP];
    show sym;
    show startP;
    n: marketMinutes;
    returns: 0.001 * sums (n?1.0) - 0.5; 
    prices: startP * 1 + returns;
    ([] date: dt; bar_time: barTimes; sym: n#sym; price: prices)
 };

show "openP 2";
show openP;


construct_table_one_day: {[dt; openP]
    show dt;
    show type openP;
    show openP;
    / show each flip (syms; openP[syms]);
    / open_prices:openP[syms]
    // t: raze { [dt; sym; op] simulate_day_one_sym[dt; sym; op] }[dt] ' [syms; openP]; 
    // t: raze { [dt; op; sym] simulate_day_one_sym[dt; sym; op[sym]] }[dt;openP] 'syms; 

    / result: raze { ([] date:x; sym:key y; price:value y) }'[key openprice; value openprice]

    // result: raze { ([] date:10#x; sym:key y; price:value y) }'[date; openPrice]
    // t: ,/ { [dt; sym; startP] simulate_day_one_sym[dt; sym; startP] }[dt] ' [syms; openP[syms]]; 
    // `date`bar_time xasc t

    show "beginning while";
    i: 0;
    while[i < count syms; 
        / sf[x; y; z[i]]; 
        show syms[i];
        show openP[syms[i]];
        tnew: simulate_day_one_sym[dt; syms[i]; openP[syms[i]]]
        i: i + 1];
    show "tnew";
    show tnew;
    show "stopping here"
    \

 };

//If dbPath doesn't exist then make it.
if[()~key dbPath; .[system;("mkdir \"",dbPathStr,"\"");{}]];


i: 0;
/ while[i < count tradingDays;

show "openP 3";
show openP;
while[i < 447;
    
    show "openP 3b";
    show i;
    show openPrice;
    dt: tradingDays[i];
    show "beginning"
    show dt;
    show "openP 4";
    show openPrice;
    day_table: construct_table_one_day[dt; openPrice];
    day_table_2: select bar_time, sym, price from day_table; 
    prices_table_total:: select bar_time, sym, price from day_table; 
    
    / .Q.dpft[dbPath; dt; `sym; `prices_table]; 
    / extr[prices_table;`a`m] replaces prices_table
    / dbPath_am replaces dbPath    
    / prices_table:: extr[day_table_2;`a`m];
    / .Q.dpft[dbPath_am; dt; `sym; `prices_table];
    / prices_table:: extr[day_table_2;`n`z];
    / .Q.dpft[dbPath_nz; dt; `sym; `prices_table];

    // You run it once per partition/date, right before you set that date’s table.
    type day_table_2;
    show "day_table_2 am";
    show dt;
    show -10#day_table_2;
    t1:.Q.en[`:/Users/zacharydugan/q/big_sim_db/db; day_table_2];
    show "t1 am";
    show dt;
    show -10#t1;
    prices_table:: extr[t1;`a`m];
    (hsym `$raze (dbPathStr_am; "/"; string dt; "/prices_table/")) set prices_table;
    prices_table:: extr[t1;`n`z];
    show "prices nz";
    show dt;
    show -10#prices_table;
    (hsym `$raze (dbPathStr_nz; "/"; string dt; "/prices_table/")) set prices_table;

    openPrice: exec last price by sym from prices_table_total; 
    if[dt=2024.09.16; show "Problem date,  openPrices "];
    if[dt=2024.09.16; show openPrice];
    if[0 = i mod 50; show "Partition saved: ", string dt];
    if[dt=2024.09.17; show "Problem date, exiting "];
    if[dt=2024.09.17; \];
    
    i+: 1
 ];

prices_table:: prices_table_total

/ __________________________________________________________________________________________________________
/ --- SECTION 3: Step 2 - Returns Calculation --- Calculates and saves ret_table
show "Step 2: Calculating Returns...";
\l /Users/zacharydugan/q/big_sim_db/db

/ p: `date`sym`bar_time xasc select from prices_table where date within (startDate; endDate); 
p: `date`sym`bar_time xasc select from prices_table; 
returns_table: update return: (price - prev price) % prev price by date, sym from p;
//  show "stopping here"
// \
{ [dt]
    / ret_table:: select date, bar_time, sym, return from returns_table where date = dt;
    / .Q.dpft[dbPath; dt; `sym; `ret_table]
    ret_table_total: select date, bar_time, sym, return from returns_table where date = dt;

    t1:.Q.en[`:/Users/zacharydugan/q/big_sim_db/db; ret_table_total];
    ret_table:: extr[t1;`a`m];
    (hsym `$raze (dbPathStr_am; "/"; string dt; "/ret_table/")) set ret_table;
    ret_table:: extr[t1;`n`z];
    (hsym `$raze (dbPathStr_nz; "/"; string dt; "/ret_table/")) set ret_table;

    // ret_table:: extr[ret_table_total;`a`m];
    // .Q.dpft[dbPath_am; dt; `sym; `ret_table];
    // ret_table:: extr[ret_table_total;`n`z];
    // .Q.dpft[dbPath_nz; dt; `sym; `ret_table];
    
 } each tradingDays;

\cd ../../cz/cz
show "stopping here after return calculation"
\
/ __________________________________________________________________________________________________________


/ __________________________________________________________________________________________________________
/ --- SECTION 4: Step 3 - Moving Medians --- Creates and saves moving_median_table
show "Step 3: Calculating Moving Medians...";
\l /Users/zacharydugan/q/big_sim_db

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
\l /Users/zacharydugan/q/big_sim_db

/ Use the moving_median_table as input for the EMA
// ema_data: select from moving_median_table;
// ema_data: update exp_median: (6#0n), ema[0.333; 6_moving_median] by sym, bar_time from ema_data;

ema_data: select from moving_median_table where date > 2023.01.10;  / 7 day moving median
/ Apply EMA (alpha 0.333) to the moving median values, ignoring the first 6 nulls
ema_data: update exp_median: 0n from ema_data    / float null
dbg:1b
ema_moving_median:{[input_sym; input_bar_time]
    moving_median_array: exec moving_median from ema_data where (sym=input_sym) and (bar_time=input_bar_time);
    input_array: ema[0.333; moving_median_array];
    cond1: where (ema_data`bar_time)=input_bar_time;
    cond2: where (ema_data`sym)=input_sym;
    inds: cond1 inter cond2;
    if[(count inds) <> (count input_array); '"ema_moving_median: length mismatch (inds vs input_array)"];
    amended_col: @[ema_data`exp_median; inds; :; "f"$input_array];
    ema_data:: update exp_median: amended_col from ema_data;
    moving_median_array
    }

pairs: distinct flip `sym`bar_time!(ema_data`sym; ema_data`bar_time);
n: count pairs;
run_one: {[idx]
  sym_idx: pairs[`sym][idx];
  time_idx: pairs[`bar_time][idx];
  ema_moving_median[sym_idx; time_idx]
 };
run_one each til n

/ ema_data: update exp_median: (6#0n), ema[0.333; moving_median] by sym, bar_time from ema_data;

/ ema_data: update exp_median: (6#0n), ema[0.333; moving_median] by sym, bar_time from ema_data;
/ ema_data: update exp_median: (6#0n), ema[0.333; moving_median] from ema_data where (sym=sym) and (bar_time=bar_time);
/ ema_data: update exp_median: ema[0.333; moving_median] from ema_data where (sym=sym) and (bar_time=bar_time)

show "Showing ema results"
show 20#(ema_data)
show -20#(ema_data)
// show "stopping here"
// \

{ [dt]
    exp_median_table:: select date, bar_time, sym, moving_median, exp_median from ema_data where date = dt;
    .Q.dpft[dbPath; dt; `sym; `exp_median_table]
 } each tradingDays;
/ __________________________________________________________________________________________________________


/ __________________________________________________________________________________________________________
 / --- SECTION 5: Step 4 - Exponential Medians (Nudge Logic) ---
show "Step 5: Applying Exponential Nudge Median...";
\l /Users/zacharydugan/q/big_sim_db

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
/ temp_table: select date, sym, bar_time, price from prices_table;
/ Calculate Exponential Median using scan iterator (\)
/ It nudges the median toward the price based on alpha
/ median_results: update exp_med: exp_step[alpha;;]\[first price; price] by date, sym from temp_table;

/ Load temporary table for calculation
temp_table: select date, sym, bar_time, return from ret_table;
/ Calculate Exponential Median using scan iterator (\)
/ It nudges the median toward the price based on alpha
median_results: update exp_med: exp_step[alpha;;]\[first return; return] by date, sym from temp_table;


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

\l /Users/zacharydugan/q/big_sim_db

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
/ p: exec exp_median from exp_median_table where sym=sym_filter, date > date_filter;
// p: select exp_median from exp_median_table where sym=sym_filter, date > date_filter;
// p2: exec exp_median from p
// m: select exp_med from exp_median_nudge_table where (sym=sym_filter), (date>date_filter);
// m2: exec exp_med from m

// / --- 3. Run the correlation ---
// / This will return a single float between -1 and 1
// result: p2 cor m2;

// show "Correlation for ", (string sym_filter), " since ", (string date_filter), ":";
// show result;

// // / --- SECTION 6: Final Correlation Analysis ---
// show "Step 5: Generating Correlation Report...";

// / Calculate for all symbols in the last 21 minutes of the latest date
// latest_dt: last tradingDays;
// latest_data: select from exp_median_table where date = latest_dt;

// / Generate a table showing correlation for all symbols
// final_report: select 
//     last_21_cor: (-21#price) cor (-21#exp_med),
//     full_day_cor: price cor exp_med 
//     by sym from latest_data;

// show "Final Correlation Report for ", string latest_dt;
// show final_report;

// show "--- Pipeline Completed Successfully ---";
