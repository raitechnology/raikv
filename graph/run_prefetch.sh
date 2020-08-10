#!/bin/bash

. $(dirname $0)/kv_test_functions.sh

/bin/rm -f pref_1.txt pref_2.txt pref_4.txt pref_8.txt

echo ...this will take 10 minutes...

fn_prefetch 4
fn_prefetch 8
fn_prefetch 16
fn_prefetch 32
fn_prefetch 64
fn_prefetch 128
fn_prefetch 256
fn_prefetch 512
fn_prefetch 1024
fn_prefetch 2048
fn_prefetch 4096
fn_prefetch 8192
fn_prefetch 16384
fn_prefetch 32768
fn_prefetch 65536
fn_prefetch 131072

fn_done
