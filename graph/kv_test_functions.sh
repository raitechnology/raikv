#!/bin/bash

export osver=$(lsb_release -rs | sed 's/[.].*//')
export osdist=$(lsb_release -i | sed 's/.*:\t//')

if [ "${osdist}x" == "Fedorax" ] ; then
  export ospref=FC
elif [ "${osdist}x" == "Ubuntux" ] ; then
  export ospref=UB
elif [ "${osdist}x" == "Debianx" ] ; then
  export ospref=DEB
else
  export ospref=RH
fi

export PATH=../${ospref}${osver}_x86_64/bin:$PATH

if ! command -v kv_test &> /dev/null
then
  echo "kv_test could not be found"
  exit
fi

export KVMARG="-m sysv:my_test"
export NCPU=$(grep -c '^processor' /proc/cpuinfo)

function fn_done() {
  echo done, to remove kv shm, run:  kv_server -r -m $KVMARG
}

# usage: fn_prefetch <kv-map-size>
function fn_prefetch() {
  echo prefetch $1 $KVMARG
  touch pref_1.txt pref_2.txt pref_4.txt pref_8.txt
  kv_test $KVMARG -p 100 -c $1 -x int -o find -n 10 -f 1 -q | tee -a pref_1.txt
  kv_test $KVMARG -p 100       -x int -o find -n 10 -f 2 -q | tee -a pref_2.txt
  kv_test $KVMARG -p 100       -x int -o find -n 10 -f 4 -q | tee -a pref_4.txt
  kv_test $KVMARG -p 100       -x int -o find -n 10 -f 8 -q | tee -a pref_8.txt
}

# usage: fn_mt_read <hashtable-load> <kv-map-size> <key-generator>
function fn_mt_read() {
  echo populating $2 $KVMARG for read $1 using $3 keys
  kv_test $KVMARG -p $1 -c $2 -x fill -f 8 -q
  kv_test $KVMARG -p $1 -t  1 -x $3 -o find -n 10 -f 8 -q | tee mtr${1}_${2}.txt
  kv_test $KVMARG -p $1 -t  2 -x $3 -o find -n 10 -f 8 -q | tee -a mtr${1}_${2}.txt
  X=4
  while [ $X -le $NCPU ] ; do
  kv_test $KVMARG -p $1 -t $X -x $3 -o find -n 10 -f 8 -q | tee -a mtr${1}_${2}.txt
  X=$(($X + 4))
  done
}

# usage: fn_mt_write <hashtable-load> <kv-map-size> <key-generator>
function fn_mt_write() {
  echo populating $2 $KVMARG for write $1 using $3 keys
  kv_test $KVMARG -p $1 -c $2 -x fill -f 8 -q
  kv_test $KVMARG -p $1 -t  1 -x $3 -o ins -n 10 -f 8 -q | tee mtw${1}_${2}.txt
  kv_test $KVMARG -p $1 -t  2 -x $3 -o ins -n 10 -f 8 -q | tee -a mtw${1}_${2}.txt
  X=4
  while [ $X -le $NCPU ] ; do
  kv_test $KVMARG -p $1 -t $X -x $3 -o ins -n 10 -f 8 -q | tee -a mtw${1}_${2}.txt
  X=$(($X + 4))
  done
}

# usage: fn_mt_ratio <hashtable-load> <kv-map-size> <find-pct> <key-generator>
function fn_mt_ratio() {
  echo populating $2 $KVMARG for load $1 ratio $3 using $4 keys
  kv_test $KVMARG -p $1 -c $2 -x fill -f 8 -q
  kv_test $KVMARG -p $1 -t  1 -x $4 -o ratio -r $3 -n 10 -f 8 -q | tee mtx${1}_${2}_${3}_${4}.txt
  kv_test $KVMARG -p $1 -t  2 -x $4 -o ratio -r $3 -n 10 -f 8 -q | tee -a mtx${1}_${2}_${3}_${4}.txt
  X=4
  while [ $X -le $NCPU ] ; do
  kv_test $KVMARG -p $1 -t $X -x $4 -o ratio -r $3 -n 10 -f 8 -q | tee -a mtx${1}_${2}_${3}_${4}.txt
  X=$(($X + 4))
  done
}

