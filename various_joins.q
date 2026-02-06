/ \l clean.q

/ Table A: The Reference (Analysis)
sales:([] 
    time:10:00 10:05 10:10; 
    sym:`AAPL`AAPL`MSFT; 
    price:150.1 150.2 400.5; 
    qty:100 200 150);

/ Table B: The Complement (Audit) - Note: different column names/types
fees:([] 
    t:10:00 10:05 10:15;               / Missing 10:10, adds 10:15
    s:("AAPL";"AAPL";"GOOG");          / Strings instead of Symbols
    comm:0.01 0.02 0.05);

/ Table C: The Targets (Targets)
targets:([sym:`AAPL`MSFT`GOOG] 
    goal:500 500 500);

show "sales"
show sales
show "fees"
show fees
show "targets"
show targets

/ ___________________________________
/ IJ

// myNames: `time`sym!`t`s;
myNames: `t`s!`time`sym;
// show "myNames"
// show myNames
// myTypes: `time`sym ! "ts"; 
// fee_cleaned: cleanAndAlign[fees; myNames; myTypes];
// show fee_cleaned

fees: myNames xcol fees
fees: update sym: `$'sym from fees

ijt: sales ij `time`sym xkey fees;

/ ___________________________________
/ UJ
ujt: sales uj fees;
show ujt
/ ___________________________________


// "Using the sales and targets tables, how do I join them 
// on the symbol (sym) column so that every sale displays 
// its respective goal?"
/ ___________________________________
/ EJ
ejt: ej[`sym; sales; targets];
show ejt
/ ___________________________________


// "How can I generate a report that lists every single 
// trade from the sales table, and for each trade, 
// shows the corresponding goal from the targets table—even 
// if a specific symbol doesn't have a target defined?"
/ ___________________________________
/ LJ
ljt: sales lj targets;
show ljt
/ ___________________________________
