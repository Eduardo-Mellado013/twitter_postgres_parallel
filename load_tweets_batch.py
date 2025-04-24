#!/usr/bin/python3

import sqlalchemy
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################

def remove_nulls(s):
    if s is None:
        return None
    return s.replace('\x00', '\\x00')

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def _bulk_insert_sql(table, rows):
    if not rows:
        raise ValueError('Must be at least one dictionary in the rows variable')
    keys = set(rows[0].keys())
    for row in rows:
        if set(row.keys()) != keys:
            raise ValueError('All dictionaries must contain the same keys')

    cols = ','.join(keys)
    placeholders = ','.join(
        '(' + ','.join(f':{key}{i}' for key in keys) + ')'
        for i in range(len(rows))
    )
    sql = f"""
        INSERT INTO {table} ({cols})
        VALUES {placeholders}
        ON CONFLICT DO NOTHING
    """
    binds = { f"{key}{i}": value
              for i,row in enumerate(rows)
              for key,value in row.items() }
    return (' '.join(sql.split()), binds)

def bulk_insert(connection, table, rows):
    if not rows:
        return
    sql, binds = _bulk_insert_sql(table, rows)
    connection.execute(sqlalchemy.sql.text(sql), binds)

################################################################################
# main loader functions
################################################################################

def insert_tweets(connection, tweets, batch_size=1000):
    """
    Splits tweets into batches and inserts each batch
    within the outer transaction managed by __main__.
    """
    for i, tweet_batch in enumerate(batch(tweets, batch_size)):
        print(datetime.datetime.now(), 'insert_tweets batch=', i)
        _insert_tweets(connection, tweet_batch)

def _insert_tweets(connection, input_tweets):
    users = []
    tweets = []
    users_unhydrated_from_tweets = []
    users_unhydrated_from_mentions = []
    tweet_mentions = []
    tweet_tags = []
    tweet_media = []
    tweet_urls = []

    # STEP 1: build row-dicts
    for tweet in input_tweets:
        # USERS
        users.append({
            'id_users': tweet['user']['id'],
            'created_at': tweet['user']['created_at'],
            'updated_at': tweet['created_at'],
            'screen_name': remove_nulls(tweet['user']['screen_name']),
            'name': remove_nulls(tweet['user']['name']),
            'location': remove_nulls(tweet['user']['location']),
            'url': remove_nulls(tweet['user']['url']),
            'description': remove_nulls(tweet['user']['description']),
            'protected': tweet['user']['protected'],
            'verified': tweet['user']['verified'],
            'friends_count': tweet['user']['friends_count'],
            'listed_count': tweet['user']['listed_count'],
            'favourites_count': tweet['user']['favourites_count'],
            'statuses_count': tweet['user']['statuses_count'],
            'withheld_in_countries': tweet['user'].get('withheld_in_countries', None),
        })

        # TWEETS (geo + text)
        try:
            coords = tweet['geo']['coordinates']
            geo_coords = f"{coords[0]} {coords[1]}"
            geo_str = 'POINT'
        except Exception:
            geo_coords = None
            geo_str = None

        try:
            text = tweet['extended_tweet']['full_text']
        except KeyError:
            text = tweet['text']

        country_code = None
        try:
            country_code = tweet['place']['country_code'].lower()
        except Exception:
            pass

        state_code = None
        if country_code == 'us':
            sc = tweet['place']['full_name'].split(',')[-1].strip().lower()
            if len(sc) <= 2:
                state_code = sc

        place_name = None
        try:
            place_name = tweet['place']['full_name']
        except Exception:
            pass

        # handle unhydrated reply-users
        if tweet.get('in_reply_to_user_id') is not None:
            users_unhydrated_from_tweets.append({
                'id_users': tweet['in_reply_to_user_id'],
                'screen_name': remove_nulls(tweet.get('in_reply_to_screen_name'))
            })

        tweets.append({
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet['created_at'],
            'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
            'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
            'quoted_status_id': tweet.get('quoted_status_id'),
            'geo_str': geo_str,
            'geo_coords': geo_coords,
            'retweet_count': tweet.get('retweet_count'),
            'quote_count': tweet.get('quote_count'),
            'favorite_count': tweet.get('favorite_count'),
            'withheld_copyright': tweet.get('withheld_copyright'),
            'withheld_in_countries': tweet.get('withheld_in_countries'),
            'place_name': place_name,
            'country_code': country_code,
            'state_code': state_code,
            'lang': tweet.get('lang'),
            'text': remove_nulls(text),
            'source': remove_nulls(tweet.get('source'))
        })

        # TWEET_URLS
        urls = tweet.get('extended_tweet', {}).get('entities', {}).get('urls',
               tweet['entities']['urls'])
        for u in urls:
            tweet_urls.append({
                'id_tweets': tweet['id'],
                'url': remove_nulls(u['expanded_url'])
            })

        # TWEET_MENTIONS
        mentions = tweet.get('extended_tweet', {}).get('entities', {}).get('user_mentions',
                   tweet['entities']['user_mentions'])
        for m in mentions:
            users_unhydrated_from_mentions.append({
                'id_users': m['id'],
                'name': remove_nulls(m['name']),
                'screen_name': remove_nulls(m['screen_name'])
            })
            tweet_mentions.append({
                'id_tweets': tweet['id'],
                'id_users': m['id']
            })

        # TWEET_TAGS
        hashtags = tweet.get('extended_tweet', {}).get('entities', {}).get('hashtags',
                   tweet['entities']['hashtags'])
        cashtags = tweet.get('extended_tweet', {}).get('entities', {}).get('symbols',
                   tweet['entities']['symbols'])
        tags = [f"#{h['text']}" for h in hashtags] + [f"${c['text']}" for c in cashtags]
        for tag in tags:
            tweet_tags.append({
                'id_tweets': tweet['id'],
                'tag': remove_nulls(tag)
            })

        # TWEET_MEDIA
        media = tweet.get('extended_tweet', {}).get('extended_entities', {}).get('media',
                tweet.get('extended_entities', {}).get('media', []))
        for m in media:
            tweet_media.append({
                'id_tweets': tweet['id'],
                'url': remove_nulls(m['media_url']),
                'type': m['type']
            })

    # STEP 2: bulk-insert all lists in a single outer transaction
    bulk_insert(connection, 'users', users)
    bulk_insert(connection, 'users', users_unhydrated_from_tweets)
    bulk_insert(connection, 'users', users_unhydrated_from_mentions)
    bulk_insert(connection, 'tweet_mentions', tweet_mentions)
    bulk_insert(connection, 'tweet_tags', tweet_tags)
    bulk_insert(connection, 'tweet_media', tweet_media)
    bulk_insert(connection, 'tweet_urls', tweet_urls)

    # Tweets need ST_GeomFromText on insertion
    sql = sqlalchemy.sql.text(
        "INSERT INTO tweets "
        "(id_tweets,id_users,created_at,in_reply_to_status_id,in_reply_to_user_id,"
        "quoted_status_id,geo,retweet_count,quote_count,favorite_count,"
        "withheld_copyright,withheld_in_countries,place_name,country_code,"
        "state_code,lang,text,source) VALUES "
        + ",".join(
            f"(:id_tweets{i},:id_users{i},:created_at{i},:in_reply_to_status_id{i},"
            f":in_reply_to_user_id{i},:quoted_status_id{i},"
            f"ST_GeomFromText(:geo_str{i} || '(' || :geo_coords{i} || ')'),"
            f":retweet_count{i},:quote_count{i},:favorite_count{i},"
            f":withheld_copyright{i},:withheld_in_countries{i},"
            f":place_name{i},:country_code{i},:state_code{i},:lang{i},"
            f":text{i},:source{i})"
            for i in range(len(tweets))
        )
        + " ON CONFLICT DO NOTHING"
    )
    binds = { f"{key}{i}": value
              for i, tw in enumerate(tweets)
              for key, value in tw.items() }
    connection.execute(sql, binds)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--batch_size', type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(
        args.db,
        connect_args={'application_name': 'load_tweets_batch.py'}
    )
    connection = engine.connect()

    # ONE outer transaction: each file’s worth of batches runs within this
    with connection.begin():
        for filename in sorted(args.inputs, reverse=True):
            print(datetime.datetime.now(), filename)
            with zipfile.ZipFile(filename, 'r') as archive:
                for subfilename in sorted(archive.namelist(), reverse=True):
                    with io.TextIOWrapper(archive.open(subfilename)) as f:
                        tweets = [json.loads(line) for line in f]
                        insert_tweets(connection, tweets, args.batch_size)

