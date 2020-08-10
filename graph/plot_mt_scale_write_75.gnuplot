#!/usr/bin/gnuplot

set title "Rai KV Write 75% Load:  kv-test -p 75 -c $SZ -t $MT -x rand -o ins -n 10 -f 8 (SZ 256->131072, MT 1->NCPU)"
set xlabel "Number of Threads"
set ylabel "Inserts per Second (Millions)"
set yrange [0:1000]
set ytic 100
set grid

plot "mtw75_256.txt"    using 1:($5/1000000) with linespoints title "HT 256 MB", \
     "mtw75_1024.txt"   using 1:($5/1000000) with linespoints title "HT 1 GB", \
     "mtw75_4096.txt"   using 1:($5/1000000) with linespoints title "HT 4 GB", \
     "mtw75_16384.txt"  using 1:($5/1000000) with linespoints title "HT 16 GB", \
     "mtw75_32768.txt"  using 1:($5/1000000) with linespoints title "HT 32 GB", \
     "mtw75_65536.txt"  using 1:($5/1000000) with linespoints title "HT 64 GB", \
     "mtw75_131072.txt" using 1:($5/1000000) with linespoints title "HT 128 GB"

