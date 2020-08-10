#!/bin/bash

. $(dirname $0)/kv_test_functions.sh

echo ...this will take 10 minutes...

fn_mt_ratio 50 256 90 rand
fn_mt_ratio 50 256 50 rand
fn_mt_ratio 50 1024 90 rand
fn_mt_ratio 50 1024 50 rand
fn_mt_ratio 50 4096 90 rand
fn_mt_ratio 50 4096 50 rand
fn_mt_ratio 50 8192 90 rand
fn_mt_ratio 50 8192 50 rand
fn_mt_ratio 50 16384 90 rand
fn_mt_ratio 50 16384 50 rand
fn_mt_ratio 50 32768 90 rand
fn_mt_ratio 50 32768 50 rand
fn_mt_ratio 50 65536 90 rand
fn_mt_ratio 50 65536 50 rand
fn_mt_ratio 50 131072 90 rand
fn_mt_ratio 50 131072 50 rand

fn_done
