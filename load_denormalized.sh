#!/bin/sh

unzip -p "$1" | sed 's/\\u0000//g' | psql postgresql://postgres:pass@localhost:6343/ -c "COPY tweets_jsonb (data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
