#!/usr/bin/gnuplot

set title "Rai KV Read 75% Load:  kv-test -p 75 -t $MT -x rand -o find -n 10 -f 8 (SZ 256->131072, MT 1->NCPU)"
set xlabel "Number of Threads"
set ylabel "Finds per Second (Millions)"
set yrange [0:1000]
set ytic 100
set grid

plot "mtr75_256.txt"    using 1:($5/1000000) with linespoints title "HT 256 MB", \
     "mtr75_1024.txt"   using 1:($5/1000000) with linespoints title "HT 1 GB", \
     "mtr75_4096.txt"   using 1:($5/1000000) with linespoints title "HT 4 GB", \
     "mtr75_16384.txt"  using 1:($5/1000000) with linespoints title "HT 16 GB", \
     "mtr75_32768.txt"  using 1:($5/1000000) with linespoints title "HT 32 GB", \
     "mtr75_65536.txt"  using 1:($5/1000000) with linespoints title "HT 64 GB", \
     "mtr75_131072.txt" using 1:($5/1000000) with linespoints title "HT 128 GB"

