% KV_CLI(1) Rai User's Manual
% Chris Andreson
% August 11, 2020

# NAME

kv_cli - Rai key value client

# SYNOPSIS

kv_cli [*options*]

# DESCRIPTION

The kv_cli command attaches a shared memory segment to examine and modify
the contents.  It can display the guts of the hash entries and the value
storage in order to debug or examine it.

After starting, it will print the geometry of the memory attached or
exit with an error.  These are the options that are available after
prompting for a command.

>a key value
:   Append a value to key.

>c
:   Print stats for all contexts.  A context has counters for different
hash table operations, for example:  reads, writes, adds, drops.

>C
:   Print stats for open contexts.  Print dbs opened and which contexts are
attached to the db.

>d key
:   Tombstone key, drop key.  Releases the value associated with the key and
marks the hash entry as dropped.

>D seg#
:   Dump segment data.  The value arenas are split into segments.  Each segment
can have values stored in it.  This walks the segment memory and prints the
keys and sizes of the data stored in it.

>e key
:   Acquire key and 'E key' to release.  Holds a lock on key, for debugging
locks.

>f{get,int,hex} pos
:   Print key and value string, int, hex value of hash table position.

>g key
:   Print key value as a string.

>G seg#
:   GC segment data.  This removes holes in the segment by moving values to the
head of the segment.  This usually occurs when a new value is allocated and the
context runs out of free segment data.  In that case, it will compact the
values until there is enough space available for the value.

>h key
:   Print hex dump of key value.

>i key
:   Print int value of key by converting a binary value to a integer.

>j position
:   Jump to table position and list keys.

>k [pat]
:   List keys matching pattern or all keys if no pattern, one page at a time.
Use 'K' to go back to the start scanning at position 0.

>m key
:   Acquire and push a key to a list.  This test holding multiple locks on
several keys.  If the key is held by another context, then this will fail
and return a BUSY status.

>M
:   Pop and release a previously pushed key using 'm'.

>n key
:   Acqurie and release without assigning a value.

>o
:   Print segment offsets.  This is the basic geometry that is maintained for
each segment.  When a context allocates a value, it will be appended to the
segment unless it determines that compacting the segment is worthwhile.

>s [+exp] key value
:   Set key to value with optional expires ttl.

>P key value
:   Append value to key list.

>r file
:   Read command input from file.  Use 'R' to read quietly.

>S key
:   Fetch value list from key.

>t key
:   Tombstone key, mark the key as dropped.

>T key
:   Trim key, remove value list from key.

>u +exp key
:   Update key expires ttl.

>v
:   Validate all keys are reachable.  This walks through the hash entries and
attempts to find each key.  If a key is corrupted or a key is misplaced, this
will show these errors.

>V
:   Turn on/off verbose mode.  In verbose mode, all bytes of the hash entry and
message value are displayed.  This shows the fields of the data structures in
hex.

>w db#
:   Switch to db number.

>W file
:   Output to file.  This is useful when the value is too large to display on
a terminal.

>.
:   Close the output to file.

>X
:   Print stats of the kv_cli context.

>y
:   Scan for broken locks by checking each pid attached to the shared memory
and releasing locks of the contexts which are orphaned by dead pids.

>z
:   Suspend pid.  This tests lock recovery.

>Z
:   Unsuspend pid.

>q
:   Quit.

# OPTIONS

-m map
:   The name of map file.  A prefix file, sysv, posix indicates the type of
shared memory.  The prefix sysv:raikv.shm is the default.

# SEE ALSO

The Rai KV source code and all documentation may be downloaded from
<https://github.com/raitechnology/raikv>.

