#!/bin/sh

files=$(find data/*)

echo '================================================================================'
echo 'load pg_denormalized'
echo '================================================================================'
# FIXME: implement this with GNU parallel

for file in data/*; do
    sh load_denormalized.sh $file
    time echo "$files" | parallel ./load_denormalized.sh
done

echo '================================================================================'
echo 'load pg_normalized'
echo '================================================================================'
# FIXME: implement this with GNU parallel

echo '================================================================================'
echo 'load pg_normalized_batch'
echo '================================================================================'
# FIXME: implement this with GNU parallel
