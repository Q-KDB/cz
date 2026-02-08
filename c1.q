\l /Users/zacharydugan/q/big_sim_db/db

sym_filter: `AAPL
time_filter: 10:20:00
date_filter: 2024.01.09

syms: `ibm`appl`tsla`msft`amzn`meta`wmt`ko`dis`nvda;
syms: asc syms
num_syms: count syms
show num_syms
sym_tot:num_syms-1 
// tsr: sum select return from ret_table where (sym=`amzn) and (bar_time > 09:30:00 )
// show tsr

i:0
while[i < num_syms;
    show syms[i];
    temp_sym: syms[i];
    tsr: sum select return from ret_table where (sym=temp_sym) and (bar_time > 09:30:00 );
    tt: select from ret_table where (sym=temp_sym) and (bar_time > 09:30:00 );
    // show 5#tt;
    show tsr;
    i+: 1
 ];


show "shifting to prices___________"
i:0
while[i < num_syms;
    show syms[i];
    temp_sym: syms[i];
    fp: first select price from prices_table where (sym=temp_sym) and (bar_time > 09:30:00 );
    lp: last select price from prices_table where (sym=temp_sym) and (bar_time > 09:30:00 );
    //tt: select from ret_table where (sym=temp_sym) and (bar_time > 09:30:00 );
    // show 5#tt;
    ret: (lp-fp)%fp;
    // show fp;
    // show lp;
    show ret;
    i+: 1
 ];
show "shifting to mins___________"
i:0
while[i < num_syms;
    show syms[i];
    temp_sym: syms[i];
    tt: select from prices_table where (sym=temp_sym) and (bar_time > 09:30:00 );
    show 5#tt;

    minimum: min select price from tt;
    show minimum;

    i+: 1
 ];

//  \cd ../../cz/cz

q)where ((tt`price) - 20.80157 ) < 1e-6
,173670
q)tt[173670]
date    | 2024.09.17
bar_time| 12:27:00
sym     | `sym$`wmt