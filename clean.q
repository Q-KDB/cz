/ --- SECTION 1: Setup ---
/ system "c 20 100";

/ --- SECTION 2: The Final Cleaning Function ---
cleanAndAlign:{[t; nameMap; typeMap]
    / 1. Rename columns
    t: nameMap xcol t;
    
    / 2. Get list of columns to fix
    colsToFix: (key typeMap) inter cols t;
    
    / 3. Use the functional form of cast: $[typeChar; data]
    / f takes the table, the column name, and the map
    f: {[tbl; c; m] 
        targetType: m c;
        / @[table; column; function; argument]
        @[tbl; c; { [typeChar; data] typeChar$data }; targetType]
    };
    
    / 4. Iterate through columns
    f[;;typeMap]/[t; colsToFix]
 };

/ --- SECTION 3: Data Setup ---
analysis:([] 
    timeBar: 10:00:00 10:15:00 10:30:00; 
    sym: `AAPL`IBM`TSLA; 
    vwap: 150.25 210.10 250.00; 
    volume: 1000 800 1200);

audit_dirty:([] 
    timeBar: 2026.02.05D10:00:00 2026.02.05D10:15:00 2026.02.05D10:30:00; 
    sym: ("AAPL";"IBM";"TSLA"); 
    vwap: 150.25 210.10 250.01; 
    vol: 1000 800 1200);

/ --- SECTION 4: Execution ---
show "--- STEP 1: CLEANING ---";

myNames: enlist[`vol] ! enlist[`volume];
myTypes: `timeBar`sym`volume ! "tsi"; 

audit_cleaned: cleanAndAlign[audit_dirty; myNames; myTypes];
audit_ready: (enlist[`vwap]!enlist[`audit_vwap]) xcol audit_cleaned;

show "--- STEP 2: JOINING ---";
/ Ensure audit_ready is keyed so ij can match
comparison: analysis ij `timeBar`sym xkey audit_ready;

/ Calculate diffs
comparison: update diff:vwap - audit_vwap from comparison;
failures: select from comparison where 1e-9 < abs diff;

show "--- FINAL REPORT ---";
show comparison;

$[0 = count failures;
    show "VERIFICATION: SUCCESS";
    [show "VERIFICATION: FAILED"; show failures]];