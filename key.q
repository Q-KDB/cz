/ --- STEP 1: Define the Parent Table (Reference) ---
/ It MUST be keyed using ([ ]) syntax
targets:([sym:`AAPL`MSFT`GOOG] 
    goal:500 500 500; 
    region:`US`US`US);

/ --- STEP 2: Define the Child Table (Transaction) ---
sales:([] 
    time:10:00 10:05 10:10; 
    sym:`AAPL`AAPL`MSFT; 
    price:150.1 150.2 400.5; 
    qty:100 200 150);

/ --- STEP 3: Establish the Foreign Key Link ---
/ This links the 'sym' column in sales to the 'targets' table
/ Syntax: `targetTable$columnName
sales: update sym:`targets$sym from sales;

/ --- STEP 4: Verification ---
/ Look at the meta. The 'f' column will now show `targets
meta sales