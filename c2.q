x:1;
y:2;
z: til 5

sf:{ [x;y;z] show x; show y; show z; };

i: 0;
while[i < count z; 
    sf[x; y; z[i]]; 
    i: i + 1];
