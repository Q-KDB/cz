\l /Users/zacharydugan/q/cz/cz/big_sim_db/db
\l /Users/zacharydugan/q/cur_files/quantile.q
\l /Users/zacharydugan/q/cur_files/winsorize.q
\l /Users/zacharydugan/q/cur_files/winsorize.q
\l /Users/zacharydugan/q/cz/cz/get_aligned.q
\cd /Users/zacharydugan/q/cz/cz/

// read into memory 
mn_table: select from ret_table

// We want to create "market" neutral returns for each stock so that for every bar time, the sum of the returns over the
// stocks is zero.  

mn_table: update return: return - avg return by date, bar_time from mn_table;

// Check
dt:2023.01.02;
b_time:09:32:00;
select sum return from mn_table where (date=dt) and (bar_time=b_time);
// looks good

// Now we've got our market neutral returns.  Next step Regression.  

// pred 1 is the sum of the mn returns from the last 15 minutes by symbol 
pred_1: select 15 msum return by sym from mn_table
pred_2: select 45 msum return by sym from mn_table;
pred_3: select 90 msum return by sym from mn_table;
// this yields 10 arrays for each stock of many data points.

// Check _____________________________________________________________________
b_time:09:48:00;
s:`ibm;
show select sum return from mn_table where (date=dt) and (bar_time within (09:33:00;09:48:00)) and (sym=s)
//______________________________________________________________________________


// Check________________________________________________________________________
mn_table: update cumsum: sums return by date,sym from mn_table;
//______________________________________________________________________________
// compare w pred_1 looks good

// Here we investigate another approach.  We put the predictors in a new table called pt (predictor table)
pt: select from mn_table;
pt: update pred_1: 15 msum return by sym from pt;
pt: update pred_2: 45 msum return by sym from pt;
pt: update pred_3: 45 msum return by sym from pt;


//Now we need to package these into an x_matrix.  We should ignore the first window respectively for each
// So now I have 3 predictors for 10 stocks.  The x_matrix will be 3x10  predictors * stocks
// At a given bar, the X matrix will 3x10

//Check
dt:2023.01.02;
tim:09:35:00;

/ Build 3xN matrix at one date/time (N = number of syms in that slice)
return_x_matrix:{[dt;bt;pt]
  s: select pred_1, pred_2, pred_3 by sym from pt where date=dt, bar_time=bt;
  if[0=count s; '"no rows for date/bar_time"];
  / The sym order of these will be important.  
  r1: exec pred_1 from s;
  r2: exec pred_2 from s;
  r3: exec pred_3 from s;
  :raze enlist each (r1; r2; r3)
  };

/ smoke test
dt: first exec date from pt;
bt: first exec bar_time from pt where date=dt;
x_mat: return_x_matrix[dt;bt;pt];
show "type x_mat";
show type x_mat;
show "type first x_mat";
show type first x_mat;
show "shape as (rows;cols)";
show (count x_mat; count first x_mat);

/ regression shape (N x 3)
x_mat_reg: flip x_mat;
show "type x_mat_reg";
show type x_mat_reg;
show "shape reg as (rows;cols)";
show (count x_mat_reg; count first x_mat_reg);

// Now we need to gather the y returns, ie the returns we are trying to target.__________________  
// This is going to be a 1 by 10 array
pt: update y_ret: 15 msum return by sym from pt;
// BUT we are going to want to target the y_ret 15 minutes into the future, not the present par.  

return_target_y_ret:{[dt;bt]
  exec y_ret from pt where (date=dt) and (bar_time = bt + 00:15:00)
  };

y_ret_array: return_target_y_ret[2023.01.02;10:00:00]

(x_out;y_out): get_aligned_data[dt;bt;00:15:00;pt];
y_out: enlist y_out;
coeffs: y_out lsq x_out;
show "coeffs";
show coeffs;

\

//________________________________________________________________________________________________

// ORDER IS IMPORTANT.  

X IS 3x10 ; 3 PREDICTORS FOR 10 STOCKS
Y IS 1x10 ;                  10 STOCKS
WE WANT THE ORDER OF ALL THE STOCKS TO BE THE SAME    
COEFS:  [3]   THESE WILL YIELD 3 COEFFICIENTS
PREDICTIONS: X_MAT times the COEFS will be [1,10], same as the Y




/ -------------------------------------------------------------------------
// X matrix: 3 predictors x 10 stocks at every bar.
// pred_1, pred_2, pred_3 are keyed by sym; value column `return` holds the
// rolling sum series (one vector per sym). At bar index i we want one value
// per sym per predictor -> 3 x 10.
// Skip first 89 bars so all three windows are full (90 msum needs 89 warmup).
// -------------------------------------------------------------------------
n_bars: count first pred_1[`return];
n_syms: count pred_1;
start_bar: 90;   / first bar where all three predictors have a full window

/ At a single bar i: 3 rows (predictors), 10 columns (stocks)
x_matrix_bar: {[i]
  (pred_1[`return][;i]; pred_2[`return][;i]; pred_3[`return][;i])
  };

/ x_matrix_bar[90]

/ All bars from start_bar: list of 3x10 matrices
/ x_matrix_all: x_matrix_bar each start_bar + til n_bars - start_bar;

/ Example: y at bar i (market-neutral returns for that bar), regress to get coeffs
/ y_bar: mn_table return at bar i, 10 values (one per sym) in same sym order as pred_*
/ coeffs: (flip x_matrix_bar[i]) lsq y_bar   -> 3 coeffs (one per predictor)
/ (flip gives 10x3 so rows = observations, cols = features for lsq)



// __________________________________________________________________________________________
// Scrap
\

x_mat: (exec pred_1 from pt where date=dt, bar_time=bt; 
        exec pred_2 from pt where date=dt, bar_time=bt; 
        exec pred_3 from pt where date=dt, bar_time=bt);



x_mat: return_x_matrix:[dt;tim;pt];

return_x_matrix:{ [dt;bt;pt]
    r1: exec pred_1 from pt where date=dt, bar_time=bt;
    r2: exec pred_2 from pt where date=dt, bar_time=bt;
    r3: exec pred_3 from pt where date=dt, bar_time=bt;
    :(r1; r2; r3) };


return_x_matrix:{[dt;bt]
    x_mat: (exec pred_1 from pt where date=dt, bar_time=bt; 
            exec pred_2 from pt where date=dt, bar_time=bt; 
            exec pred_3 from pt where date=dt, bar_time=bt);
    x_mat
};


x_matrix: (x1;x2;x3;x4;x5;x6;x7) 
coeffs: y lsq x_matrix

x_matrix_bar: [pred_1, pred_2, pred_3  each at a given bar] ; it's dimensions are 3x10
x_matrix: exec (pred_1; pred_2; pred_3) by sym from mn_table
x_matrix: exec (pred_1; pred_2; pred_3) from mn_table

// ___________________________________________________________
/ 1. Filter for your 10 target stocks
sub_table: select from mn_table where sym in target_stocks;

/ 2. Create the matrix
/ Each row in x_matrix will be a vector of 10 elements
x_matrix: (exec pred1 from sub_table; exec pred2 from sub_table; exec pred3 from sub_table);

/ 3. Run the regression
/ y must also be a vector of length 10
coeffs: y lsq x_matrix

load_x_matrix:{[dt;bar_time]


}

