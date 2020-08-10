#!/bin/bash

. $(dirname $0)/kv_test_functions.sh

echo ...this will take 10 minutes...

fn_mt_read 25 256 rand
fn_mt_read 25 1024 rand
fn_mt_read 25 4096 rand
fn_mt_read 25 8192 rand
fn_mt_read 25 16384 rand
fn_mt_read 25 32768 rand
fn_mt_read 25 65536 rand
fn_mt_read 25 131072 rand

fn_done
