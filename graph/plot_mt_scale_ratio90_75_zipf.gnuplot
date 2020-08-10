#!/usr/bin/gnuplot

set title "Rai KV Zipf 90/10 75% Load:  kv-test -c $SZ -p 75 -t $MT -x zipf -o ratio -r 90 -n 10 -f 8 -q (SZ 256->131072, MT 1->NCPU)"
set xlabel "Number of Threads"
set ylabel "Operations per Second (Millions)"
set yrange [0:1000]
set ytic 100
set grid

plot "mtx75_256_90_zipf.txt"    using 1:($5/1000000) with linespoints title "HT 256 MB", \
     "mtx75_1024_90_zipf.txt"   using 1:($5/1000000) with linespoints title "HT 1 GB", \
     "mtx75_4096_90_zipf.txt"   using 1:($5/1000000) with linespoints title "HT 4 GB", \
     "mtx75_16384_90_zipf.txt"  using 1:($5/1000000) with linespoints title "HT 16 GB", \
     "mtx75_32768_90_zipf.txt"  using 1:($5/1000000) with linespoints title "HT 32 GB", \
     "mtx75_65536_90_zipf.txt"  using 1:($5/1000000) with linespoints title "HT 64 GB", \
     "mtx75_131072_90_zipf.txt" using 1:($5/1000000) with linespoints title "HT 128 GB"

