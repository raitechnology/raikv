#!/bin/bash

. $(dirname $0)/kv_test_functions.sh

echo ...this will take 10 minutes...

fn_mt_read 75 256 rand
fn_mt_read 75 1024 rand
fn_mt_read 75 4096 rand
fn_mt_read 75 8192 rand
fn_mt_read 75 16384 rand
fn_mt_read 75 32768 rand
fn_mt_read 75 65536 rand
fn_mt_read 75 131072 rand

fn_done
