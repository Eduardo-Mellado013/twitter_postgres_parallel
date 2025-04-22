#!/bin/bash

files=$(find data/*)

echo '================================================================================'
echo 'load denormalized'
echo '================================================================================'
time for file in $files; do
    echo
    # copy your solution to the twitter_postgres assignment here
    unzip -p "$file" | sed 's/\\u0000//g' | iconv -f utf-8 -t utf-8 -c | psql "postgresql://postgres:pass@localhost:6343" -c "COPY tweets_jsonb (data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
done

echo '================================================================================'
echo 'load pg_normalized'
echo '================================================================================'
time for file in $files; do
    echo
    # copy your solution to the twitter_postgres assignment here
    python3 load_tweets.py --db "postgresql://postgres:pass@localhost:6344/postgres" --inputs "$file"
done

echo '================================================================================'
echo 'load pg_normalized_batch'
echo '================================================================================'
time for file in $files; do
    echo
    python3 -u load_tweets_batch.py --db=postgresql://postgres:pass@localhost:6347/ --inputs $file
done
