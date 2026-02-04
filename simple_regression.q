/ Simple least-squares regression using lsq
/ In q, lsq expects: (LHS) lsq (design) with SAME number of COLUMNS (columns = observations)

y: 2.1 3.9 6.2 7.8 10.1f
x: 1 2 3 4 5f

/ Design matrix: 2 rows (intercept, x), 5 columns (observations)
X: (5#1f; x)

/ Response as 1 row, 5 columns (enlist y)
Y: enlist y

/ Solve: coeffs mmu X = Y  =>  coeffs = Y lsq X
coeffs: Y lsq X

/ Fitted values: coeffs mmu X (1x2 mmu 2x5 = 1x5)
fitted: coeffs mmu X

/ Residuals: flatten fitted to vector, subtract from y
residuals: y - raze fitted

/ Display
show "Coefficients (intercept; slope):"
show coeffs
show "Fitted:"
show fitted
show "Residuals:"
show residuals
