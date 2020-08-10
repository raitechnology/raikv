#!/usr/bin/gnuplot

set title "Rai KV Ratio 90/10 25% Load:  kv-test -c $SZ -p 25 -t $MT -x rand -o ratio -r 90 -n 10 -f 8 -q (SZ 256->131072, MT 1->NCPU)"
set xlabel "Number of Threads"
set ylabel "Operations per Second (Millions)"
set yrange [0:1000]
set ytic 100
set grid

plot "mtx25_256_90_rand.txt"    using 1:($5/1000000) with linespoints title "HT 256 MB", \
     "mtx25_1024_90_rand.txt"   using 1:($5/1000000) with linespoints title "HT 1 GB", \
     "mtx25_4096_90_rand.txt"   using 1:($5/1000000) with linespoints title "HT 4 GB", \
     "mtx25_16384_90_rand.txt"  using 1:($5/1000000) with linespoints title "HT 16 GB", \
     "mtx25_32768_90_rand.txt"  using 1:($5/1000000) with linespoints title "HT 32 GB", \
     "mtx25_65536_90_rand.txt"  using 1:($5/1000000) with linespoints title "HT 64 GB", \
     "mtx25_131072_90_rand.txt" using 1:($5/1000000) with linespoints title "HT 128 GB"

