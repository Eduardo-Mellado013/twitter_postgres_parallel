#!/usr/bin/python3

# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################

def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    and there is no way to get the original twitter data back from the data in postgres.

    >>> remove_nulls('\x00')
    ''
    >>> remove_nulls('hello\x00 world')
    'hello world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','')

def get_id_urls(url, connection):
    '''
    Given a url, return the corresponding id in the urls table.
    If no row exists for the url, then one is inserted automatically.
    '''
    sql = sqlalchemy.sql.text('''
    insert into urls 
        (url)
        values
        (:url)
    on conflict do nothing
    returning id_urls
    ;
    ''')
    res = connection.execute(sql, {'url': url}).first()
    if res is None:
        sql = sqlalchemy.sql.text('''
        select id_urls 
        from urls
        where url = :url
        ''')
        res = connection.execute(sql, {'url': url}).first()
    return res[0]

def clean_dict(d):
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [clean_dict(item) for item in d]
    elif isinstance(d, str):
        return d.replace('\x00', '')
    else:
        return d

def insert_tweet(connection, tweet):
    '''
    Insert the tweet into the database.
    '''
    # skip if already loaded
    check = sqlalchemy.sql.text('''
    SELECT id_tweets 
      FROM tweets
     WHERE id_tweets = :id_tweets
    ''')
    if connection.execute(check, {'id_tweets': tweet['id']}).first():
        return

    tweet = clean_dict(tweet)

    # wrap entire tweet load in one transaction
    with connection.begin():

        ########################################
        # insert into the users table
        ########################################
        if tweet['user']['url'] is None:
            user_url_id = None
        else:
            user_url_id = get_id_urls(tweet['user']['url'], connection)

        upsert_user = sqlalchemy.sql.text('''
        INSERT INTO users
            (id_users, created_at, updated_at, url,
             friends_count, listed_count, favourites_count, statuses_count,
             protected, verified, screen_name, name, location,
             description, withheld_in_countries)
        VALUES
            (:id_users, :created_at, :updated_at, :url,
             :friends_count, :listed_count, :favourites_count, :statuses_count,
             :protected, :verified, :screen_name, :name, :location,
             :description, :withheld_in_countries)
        ON CONFLICT DO NOTHING
        ''')
        connection.execute(upsert_user, {
            'id_users': tweet['user']['id'],
            'created_at': tweet['user']['created_at'],
            'updated_at': tweet['user'].get('updated_at'),
            'url': user_url_id,
            'friends_count': tweet['user']['friends_count'],
            'listed_count': tweet['user']['listed_count'],
            'favourites_count': tweet['user']['favourites_count'],
            'statuses_count': tweet['user']['statuses_count'],
            'protected': tweet['user']['protected'],
            'verified': tweet['user']['verified'],
            'screen_name': tweet['user']['screen_name'],
            'name': tweet['user']['name'],
            'location': tweet['user']['location'],
            'description': tweet['user']['description'],
            'withheld_in_countries': tweet['user'].get('withheld_in_countries', [])
        })

        ########################################
        # insert into the tweets table
        ########################################
        # build WKT for geo
        try:
            coords = tweet['geo']['coordinates']
            wkt = f"POINT({coords[0]} {coords[1]})"
        except Exception:
            try:
                ring = tweet['place']['bounding_box']['coordinates'][0]
                if ring[0] != ring[-1]:
                    ring.append(ring[0])
                pts = ", ".join(f"{pt[0]} {pt[1]}" for pt in ring)
                wkt = f"POLYGON(({pts}))"
            except Exception:
                wkt = None

        text = remove_nulls(
            tweet.get('extended_tweet', {}).get('full_text', tweet['text'])
        )
        country_code = None
        try:
            country_code = tweet['place']['country_code'].lower()
        except Exception:
            pass

        state_code = None
        if country_code == 'us':
            sc = tweet['place']['full_name'].split(',')[-1].strip().lower()
            state_code = sc if len(sc) <= 2 else None

        place_name = tweet.get('place', {}).get('full_name')

        # ensure reply-to user exists
        if tweet.get('in_reply_to_user_id'):
            ins_user = sqlalchemy.sql.text('''
            INSERT INTO users (id_users)
            VALUES (:id)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(ins_user, {'id': tweet['in_reply_to_user_id']})

        ins_tweet = sqlalchemy.sql.text('''
        INSERT INTO tweets
            (id_tweets, id_users, created_at,
             in_reply_to_status_id, in_reply_to_user_id, quoted_status_id,
             retweet_count, favorite_count, quote_count,
             withheld_copyright, withheld_in_countries,
             source, text, country_code, state_code, lang,
             place_name, geo)
        VALUES
            (:id_tweets, :id_users, :created_at,
             :in_reply_to_status_id, :in_reply_to_user_id, :quoted_status_id,
             :retweet_count, :favorite_count, :quote_count,
             :withheld_copyright, :withheld_in_countries,
             :source, :text, :country_code, :state_code, :lang,
             :place_name, ST_GeomFromText(:wkt))
        ON CONFLICT DO NOTHING
        ''')
        connection.execute(ins_tweet, {
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet['created_at'],
            'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
            'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
            'quoted_status_id': tweet.get('quoted_status_id'),
            'retweet_count': tweet.get('retweet_count', 0),
            'favorite_count': tweet.get('favorite_count', 0),
            'quote_count': tweet.get('quote_count', 0),
            'withheld_copyright': tweet.get('withheld_copyright', False),
            'withheld_in_countries': tweet.get('withheld_in_countries', []),
            'source': tweet.get('source'),
            'text': text,
            'country_code': country_code,
            'state_code': state_code,
            'lang': tweet.get('lang'),
            'place_name': place_name,
            'wkt': wkt
        })

        ########################################
        # insert into tweet_urls
        ########################################
        urls = tweet.get('extended_tweet', {}).get('entities', {}).get('urls',
               tweet['entities']['urls'])
        for u in urls:
            url_id = get_id_urls(u['expanded_url'], connection)
            ins = sqlalchemy.sql.text('''
            INSERT INTO tweet_urls (id_tweets, id_urls)
            VALUES (:t, :u)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(ins, {'t': tweet['id'], 'u': url_id})

        ########################################
        # insert into tweet_mentions
        ########################################
        mentions = tweet.get('extended_tweet', {}).get('entities', {}).get('user_mentions',
                   tweet['entities']['user_mentions'])
        for m in mentions:
            uph = sqlalchemy.sql.text('''
            INSERT INTO users (id_users)
            VALUES (:id)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(uph, {'id': m['id']})

            ins = sqlalchemy.sql.text('''
            INSERT INTO tweet_mentions (id_tweets, id_users)
            VALUES (:t, :u)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(ins, {'t': tweet['id'], 'u': m['id']})

        ########################################
        # insert into tweet_tags
        ########################################
        hashtags = tweet.get('extended_tweet', {}).get('entities', {}).get('hashtags',
                   tweet['entities']['hashtags'])
        cashtags = tweet.get('extended_tweet', {}).get('entities', {}).get('symbols',
                   tweet['entities']['symbols'])
        tags = ['#'+h['text'] for h in hashtags] + ['$'+c['text'] for c in cashtags]
        for tag in tags:
            ins = sqlalchemy.sql.text('''
            INSERT INTO tweet_tags (id_tweets, tag)
            VALUES (:t, :tag)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(ins, {'t': tweet['id'], 'tag': tag})

        ########################################
        # insert into tweet_media
        ########################################
        media = tweet.get('extended_tweet', {}).get('extended_entities', {}).get('media',
                 tweet.get('extended_entities', {}).get('media', []))
        for m in media:
            media_id = get_id_urls(m['media_url'], connection)
            ins = sqlalchemy.sql.text('''
            INSERT INTO tweet_media (id_tweets, id_urls, type)
            VALUES (:t, :u, :type)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(ins, {'t': tweet['id'], 'u': media_id, 'type': m['type']})


################################################################################
# main
################################################################################

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--print_every', type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(
        args.db,
        connect_args={'application_name': 'load_tweets.py'},
    )
    connection = engine.connect()

    for filename in sorted(args.inputs, reverse=True):
        print(datetime.datetime.now(), filename)
        with zipfile.ZipFile(filename, 'r') as archive:
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for i, line in enumerate(f):
                        tweet = json.loads(line)
                        insert_tweet(connection, tweet)
                        if i % args.print_every == 0:
                            print(datetime.datetime.now(), subfilename, 'i=', i, 'id=', tweet['id'])

