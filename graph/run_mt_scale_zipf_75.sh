#!/bin/bash

. $(dirname $0)/kv_test_functions.sh

echo ...this will take 10 minutes...

fn_mt_ratio 75 256 90 zipf
fn_mt_ratio 75 256 50 zipf
fn_mt_ratio 75 1024 90 zipf
fn_mt_ratio 75 1024 50 zipf
fn_mt_ratio 75 4096 90 zipf
fn_mt_ratio 75 4096 50 zipf
fn_mt_ratio 75 8192 90 zipf
fn_mt_ratio 75 8192 50 zipf
fn_mt_ratio 75 16384 90 zipf
fn_mt_ratio 75 16384 50 zipf
fn_mt_ratio 75 32768 90 zipf
fn_mt_ratio 75 32768 50 zipf
fn_mt_ratio 75 65536 90 zipf
fn_mt_ratio 75 65536 50 zipf
fn_mt_ratio 75 131072 90 zipf
fn_mt_ratio 75 131072 50 zipf

fn_done
