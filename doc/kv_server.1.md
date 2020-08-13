% KV_SERVER(1) Rai User's Manual
% Chris Andreson
% August 11, 2020

# NAME

kv_server - Rai key value server

# SYNOPSIS

kv_server [*options*]

# DESCRIPTION

The kv_server command creates, attaches, removes shared memory segments as
well as monitors the processes attached to the shared memory.

After startup, kv_server continues to monitor and do these things:

1.  Every stats interval, it calculates statistics and printing these columns
    adding all of the activity by iterating through each process context and
    calculating the difference between the current and the last interval:

        op/s -- sum of get put operations per second,
        1/ops -- nanoseconds per operation,
        chns -- the number of chains traversed per operation,
        get -- read operations per second,
        put -- write operations per second,
        spin -- the number of busy wait loops per second,
        ht -- hash entry percent used, the hash load,
        va -- value memory percent used, the value load,
        entry -- count of hash entries used,
        GC -- number of entries moved per second,
        drop -- number of entries deleted per second,
        hits -- number of entries that exist per second,
        miss -- number of entries that were not found per second.

2.  Every check interval, it updates the load percentages by adding all of the
    database entries and all of the segment available sizes, and updating the
    percentage loads in the header.

3.  Every check interval, it uses kill( pid, 0 ) to determine of all of the
    processes are alive.  If they are not alive, it cleans any locks that may
    be owned by the process.  Unlocking a hash entry triggers other processes
    that may also be waiting on the lock or may also be dead.  This does not
    check the value consistency, so it is possible that the value is corrupted.

# OPTIONS

-m map
:   The name of map file.  A prefix file, sysv, posix indicates the type of
shared memory.  The sysv prefix below is the default.  The file prefix uses
open(2) and mmap(2).   The posix prefix uses shm_open(2) and mmap(2).  The sysv
prefix uses shmget(2) and shmat(2).  All of these try to use 1GB huge page size
first, then 2MB huge page size, then the default page size.

        file:/path/raikv.shm
        posix:raikv.shm
        sysv:raikv.shm

-s MB
:   The size of HT in MB.  The default is 1024, which is 1 GB.

-k ratio
:   The entry to segment memory ratio, a floating point number range 0 to 1.
The default is 0.25, which specifies 25% of the map is hash entries and 75% is
value storage.  A value of 1 is all hash table entries, and a value of 0 is all
value.  There must be some hash table, so 0 is not valid, but it is possible to
create a map that is all hash entries without value storage.

-c a+b
:   The cuckoo hash arity and buckets, default 2+4.  The arity are the number
of different locations, and buckets are the number hash entries per location.
Each arity uses a different hash to locate the buckets.  In the case of arity 2
The 128 bit hash is split into 64 bit hashes, one for each arity.

-o mode
:   The permissions used to create the shared memory, default ug+rw.  If an
application user id is not the same as the kv_server which created it, then the
user should belong to the group of kv_server or the other permissions should be
read and write, ugo+rw.

-v value-sz
:   The maximum value size or minimum segment size in KB, default is 2048 or 1
MB.  This is used to calculate the segment sizes.  The maximum value size is
not enforced, but the minimum segment size does enforce a limit.  Each segment
in the value storage has a thread affinity so that threads writing values will
use the same segment until it is filled.

-e entry-sz
:   The hash entry size, a multiple of 64, default is 64.  A larger entry size
could be used to store lots of small values if there is a uniform size.

-a
:   Attach to the shared memory.  This option causes the options above to
be ignored.  The geometry of the shared memory can be derived from the header
of the map.

-r
:   Remove the shared memory.  This option also causes options above to be
ignored, the map is removed.

-i secs
:   The stats interval used to summarise activity.  The rates per second over
the interval are printed to stdout.

-x secs
:   The check interval used to calculate load and check for broken locks by
using kill( pid, 0 ) to verify all of the process ids attached to are still
alive.

# SEE ALSO

The Rai KV source code and all documentation may be downloaded from
<https://github.com/raitechnology/raikv>.

