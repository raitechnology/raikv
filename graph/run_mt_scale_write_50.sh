#!/bin/bash

. $(dirname $0)/kv_test_functions.sh

echo ...this will take 10 minutes...

fn_mt_write 50 256 rand
fn_mt_write 50 1024 rand
fn_mt_write 50 4096 rand
fn_mt_write 50 8192 rand
fn_mt_write 50 16384 rand
fn_mt_write 50 32768 rand
fn_mt_write 50 65536 rand
fn_mt_write 50 131072 rand

fn_done
