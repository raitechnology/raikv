% KV_TEST(1) Rai User's Manual
% Chris Andreson
% August 11, 2020

# NAME

kv_test - Rai key value test

# SYNOPSIS

kv_test [*options*]

# DESCRIPTION

The kv_test is intended to generate load on a shared memory hash table in order
to quickly determine how this KV system performs under different configuration
scenarios.


# OPTIONS

-m map
:   The name of map file.  A prefix file, sysv, posix indicates the type of
shared memory.  The prefix sysv:raikv.shm is the default.

-c MB
:   The size of HT in MB to create.  Without this argument, the shared memory
is attached, and not created.  An value of 1024 would create a table size
of 1 GB with only hash entries and without value storage.

-t num-thr
:   The number of threads to execute simultaneously, each doing the same test.
If only one thread is run without a time limit, then the rate is printed as
the test is run.

-x test
:   The test kind.  This determines the how the key is generated.

        one  -- skips hashing and uses the same key and hits the
                same location,
        int  -- the keys are incrementing integers, the lowest
                possible key generation overhead,
        rand -- the keys are output from a random number generator,
                which helps generate keys that are evenly
                distributed using multiple threads, a sequential
                increment helps the CPU caches,
        zipf -- the keys are random but skewed by the YCSB zipf
                distribution,
        fill -- just fills the -p pct hash entries with integers
                and stops.

-p pct
:   Percent coverage of total count of hash entries, a number between 0 to 100,
the default is 50.  The keys are 8 byte integers from 0 to count / 100 * pct
where count is the number of hash table entries.

-r ratio
:   The ratio of find to insert when the -o oper is ratio.  The default is 90%,
which produces 9 find operations to 1 insert operation, randomly ordered.

-f prefetch
:   The number of prefetches to perform, default is 1.  With more than one
prefetch, the operations are batched into prefetch groups in order to overlap
the hashing and the prefetching, hiding memory latency.

-o oper
:   The type of opereration to perform on the keys.

        find  -- lookup key for reading,
        ins   -- insert key value,
        ratio -- uses -r ratio for randomly ordered find + ins

-n secs
:   The number of seconds to run.  If not specified, it runs forever.  Use this
with one thread to show stats on stdout until ctrl-c.

-d db-num
:   database number to use, default is zero, the range is 0 -> 255.

# SEE ALSO

The Rai KV source code and all documentation may be downloaded from
<https://github.com/raitechnology/raikv>.

