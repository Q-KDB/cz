/ --- SECTION 1: Create Table 1 (Analysis) ---------------------------------------------------------------------------
analysis:([] 
    timeBar: 10:00:00 10:15:00 10:30:00; 
    sym: `AAPL`AAPL`AAPL; 
    vwap: 150.25 150.50 150.75; 
    volume: 1000 1500 1200);

show "--- ORIGINAL TABLE: analysis ---";
show analysis;
/ Check types: 't' for time, 's' for symbol, 'f' for float, 'j' for long
show meta analysis; 

/ --- SECTION 2: Create Table 2 (Audit) ---------------------------------------------------------------------------
/ This uses 'vol' instead of 'volume'
audit_table:([] 
    timeBar: 10:00:00 10:15:00 10:30:00; 
    sym: `AAPL`AAPL`AAPL; 
    vwap: 150.25 150.50 150.75; 
    vol: 1000 1500 1200);

show "--- ORIGINAL TABLE: audit_table ---";
show audit_table;
show meta audit_table;

/ --- SECTION 3: The Renaming Logic ------------------------------------------------------------------------
show "--- RENAMING 'vol' to 'volume' ---";
audit_aligned:(enlist[`vol]!enlist[`volume]) xcol audit_table;

/ Change vwap name to avoid collision during join
audit_aligned:(enlist[`vwap]!enlist[`audit_vwap]) xcol audit_aligned;

show audit_aligned;

/ --- SECTION 4: Safe Join (Casting to prevent type error) ---------------------------------------------------------------------------
/ We cast timeBar to 'time' and sym to 'symbol' to ensure types match
comparison:(update timeBar:`time$timeBar, sym:`symbol$sym from analysis) ij 
           `timeBar`sym xkey (update timeBar:`time$timeBar, sym:`symbol$sym from audit_aligned);

// show "--- RESULTING COMPARISON TABLE ---";
show comparison;

/ -------------------------------------------------------------------------
/ -------------------------------------------------------------------------
/ -------------------------------------------------------------------------
/ Take 2
show " "
show "Take 2"
show " "

/ --- SECTION 1: Create Table 1 (Analysis) ---------------------------------------------------------------------------
analysis:([] 
    date: 2023.01.01 2023.01.02 2023.01.03; 
    timeBar: 10:00:00 10:15:00 10:30:00; 
    sym: `AAPL`AAPL`AAPL; 
    high: 152.25 152.50 152.75; 
    vwap: 150.25 150.50 150.75; 
    volume: 1000 1500 1200);

show "--- ORIGINAL TABLE: analysis ---";
show analysis;
/ Check types: 't' for time, 's' for symbol, 'f' for float, 'j' for long
show meta analysis; 

/ --- SECTION 2: Create Table 2 (Audit) ---------------------------------------------------------------------------
/ This uses 'vol' instead of 'volume'
audit_table:([] 
    date: 2023.01.04 2023.01.05 2023.01.06 ;
    timeBar: 10:00:00 10:15:00 10:30:00; 
    sym: `AAPL`AAPL`AAPL; 
    hi: 152.25 152.50 152.75;
    vwap: 150.25 150.50 150.75; 
    vol: 1000 1500 1200);

show "--- Audit TABLE: audit_table ---";
show audit_table;
show meta audit_table;

/ -------------------------------------------------------------------------
audit_table_meta: meta audit_table;
analy_table_meta: meta analysis; 

show "Are the metas equal?"
show audit_table_meta = analy_table_meta
/ -------------------------------------------------------------------------

/ --- SECTION 3: The Renaming Logic ------------------------------------------------------------------------
show "--- RENAMING 'vol' to 'volume' ---";
audit_aligned:(enlist[`vol]!enlist[`volume]) xcol audit_table;
audit_aligned:(enlist[`hi]!enlist[`high]) xcol audit_aligned;
show audit_aligned;

/ --- SECTION 4: Safe Join (Casting to prevent type error) ---------------------------------------------------------------------------
/ We cast timeBar to 'time' and sym to 'symbol' to ensure types match
comparison:(update date:`date$date, timeBar:`time$timeBar, sym:`symbol$sym from analysis) uj 
           `date xkey (update date:`date$date, timeBar:`time$timeBar, sym:`symbol$sym from audit_aligned);
result: audit_aligned uj analysis
result: `date xasc result



show "--- RESULTING COMPARISON TABLE ---";
// show comparison;



