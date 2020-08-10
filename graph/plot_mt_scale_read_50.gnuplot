#!/usr/bin/gnuplot

set title "Rai KV Read 50% Load:  kv-test -p 50 -t $MT -x rand -o find -n 10 -f 8 (SZ 256->131072, MT 1->NCPU)"
set xlabel "Number of Threads"
set ylabel "Finds per Second (Millions)"
set yrange [0:1000]
set ytic 100
set grid

plot "mtr50_256.txt"    using 1:($5/1000000) with linespoints title "HT 256 MB", \
     "mtr50_1024.txt"   using 1:($5/1000000) with linespoints title "HT 1 GB", \
     "mtr50_4096.txt"   using 1:($5/1000000) with linespoints title "HT 4 GB", \
     "mtr50_16384.txt"  using 1:($5/1000000) with linespoints title "HT 16 GB", \
     "mtr50_32768.txt"  using 1:($5/1000000) with linespoints title "HT 32 GB", \
     "mtr50_65536.txt"  using 1:($5/1000000) with linespoints title "HT 64 GB", \
     "mtr50_131072.txt" using 1:($5/1000000) with linespoints title "HT 128 GB"

