#!/usr/bin/gnuplot

set title "Rai KV Write 25% Load:  kv-test -p 25 -c $SZ -t $MT -x rand -o ins -n 10 -f 8 (SZ 256->131072, MT 1->NCPU)"
set xlabel "Number of Threads"
set ylabel "Inserts per Second (Millions)"
set yrange [0:1000]
set ytic 100
set grid

plot "mtw25_256.txt"    using 1:($5/1000000) with linespoints title "HT 256 MB", \
     "mtw25_1024.txt"   using 1:($5/1000000) with linespoints title "HT 1 GB", \
     "mtw25_4096.txt"   using 1:($5/1000000) with linespoints title "HT 4 GB", \
     "mtw25_16384.txt"  using 1:($5/1000000) with linespoints title "HT 16 GB", \
     "mtw25_32768.txt"  using 1:($5/1000000) with linespoints title "HT 32 GB", \
     "mtw25_65536.txt"  using 1:($5/1000000) with linespoints title "HT 64 GB", \
     "mtw25_131072.txt" using 1:($5/1000000) with linespoints title "HT 128 GB"

