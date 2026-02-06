/ --- SECTION 1: Generate Sample Data (If not already present) ---
/ This ensures the 'trades error does not happen
if[()~key `trades;
    show "--- Creating Sample Trades Data ---";
    trades:([] 
        timeBar: 10:00:00 10:00:00 10:15:00 10:15:00 10:30:00; 
        sym: `AAPL`AAPL`AAPL`AAPL`AAPL; 
        price: 150.10 150.40 150.50 150.50 150.75; 
        size: 500 500 700 800 1200);
    ];

/ This is your existing Analysis result we want to verify
if[()~key `analysis;
    analysis:([] 
        timeBar: 10:00:00 10:15:00 10:30:00; 
        sym: `AAPL`AAPL`AAPL; 
        vwap: 150.25 150.50 150.75; 
        volume: 1000 1500 1200);
    ];

/ --- SECTION 2: Calculate Audit Values ---
show "--- STEP 1: Calculating Audit Values from Raw Trades ---";

/ Recalculate VWAP and Total Volume from source
audit: select 
    audit_vwap: (sum price * size) % sum size, 
    audit_volume: sum size 
    by timeBar, sym from trades;

show audit;

/ --- SECTION 3: The Reconciliation Join ---
show "--- STEP 2: Joining Analysis with Audit ---";

/ ij (Inner Join) aligns the audit results side-by-side with your analysis
/ We use xkey to tell kdb+ which columns to match on
comparison: analysis ij `timeBar`sym xkey audit;

/ --- SECTION 4: Discrepancy Detection ---
show "--- STEP 3: Checking for Math Mismatches ---";

/ Calculate differences and check for errors
comparison: update 
    vwap_diff: vwap - audit_vwap, 
    vol_diff: volume - audit_volume 
    from comparison;

/ Identify any rows that do not match exactly
failures: select from comparison where (1e-9 < abs vwap_diff) or (0 <> vol_diff);

/ --- SECTION 5: Results Printing ---
show "--- FINAL COMPARISON TABLE ---";
show comparison;

show "--- VERIFICATION SUMMARY ---";
$[0 = count failures;
    show "SUCCESS: Your analysis results match the trade data perfectly.";
    [show "FAILED: Discrepancies found!"; show failures]
    ];

comparison: update volume:4000 from comparison where volume=1500
diff_array: (comparison`volume) - (comparison`audit_volume)
diff_index: where diff_array > 0
show "We have a difference in Volume on this line:"
show comparison[diff_index]

// comparison: update vol_diff: volume - audit_volume from comparison
comparison: update vol_diff: (volume <> audit_volume) from comparison
show comparison