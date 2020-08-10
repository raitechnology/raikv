#!/usr/bin/gnuplot
set title "Rai KV Prefetch Test:  kv-test -c $SZ -p 100 -x int -o find -n 10 -f $PRE (SZ 4->131072, PRE 1->8)"
set xlabel "Hashtable Size (MB)"
set ylabel "Nanoseconds Per Lookup"
set logscale x 2
set grid

plot "pref_1.txt" using 2:6 with linespoints title "No prefetching", \
     "pref_2.txt" using 2:6 with linespoints title "Prefetch 2", \
     "pref_4.txt" using 2:6 with linespoints title "Prefetch 4", \
     "pref_8.txt" using 2:6 with linespoints title "Prefetch 8"

