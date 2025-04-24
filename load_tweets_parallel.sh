#!/bin/sh

files=$(find data/*)

echo '================================================================================'
echo 'load pg_denormalized'
echo '================================================================================'
# FIXME: implement this with GNU parallel
time for file in $files; do
    echo
    parallel ./load_denormalized.sh
done

echo '================================================================================'
echo 'load pg_normalized'
echo '================================================================================'
time for file in $files; do
    parallel ./load_normalized.sh
done
echo '================================================================================'
echo 'load pg_normalized_batch'
echo '================================================================================'
time for file in $files; do
    parallel ./load_normalized_batch.sh
done
