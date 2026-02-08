
dbPathStr: "/Users/zacharydugan/q/seg_example/db";
dbPath: hsym `$dbPathStr;

extr:{[t;r] select from t where (`$1#'string sym) within r}

/ t1:.Q.en[`:/Users/zacharydugan/q/seg_example/db;] ([] ti:09:30:00 09:31:00; sym:`ibm`t; p:101 17f)
tbl:([] ti:09:30:00 09:31:00; sym:`ibm`t; p:101 17f);
type tbl
t1:.Q.en[`:/Users/zacharydugan/q/seg_example/db; tbl]
`:/Users/zacharydugan/q/seg_example/am/2015.01.01/t/ set extr[t1;`a`m]
`:/Users/zacharydugan/q/seg_example/nz/2015.01.01/t/ set extr[t1;`n`z]


t2:.Q.en[`:/Users/zacharydugan/q/seg_example/db;] ([] ti:09:30:00 09:31:00; sym:`ibm`t; p:101.5 17.5)
`:/Users/zacharydugan/q/seg_example/am/2015.01.02/t/ set extr[t2;`a`m]
`:/Users/zacharydugan/q/seg_example/nz/2015.01.02/t/ set extr[t2;`n`z]


`:/Users/zacharydugan/q/seg_example/db/par.txt 0: ("/Users/zacharydugan/q/seg_example/am"; "/Users/zacharydugan/q/seg_example/nz")

\l /Users/zacharydugan/q/seg_example/db
select from t where date within 2015.01.01 2015.01.02