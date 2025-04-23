#!/usr/bin/python3

# imports
import psycopg2
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

    The null character is extremely rarely used in real world text (approx. 1 in 1 billion tweets),
    and so this isn't too big of a deal.
    A more correct implementation, however, would be to *escape* the null characters rather than remove them.
    This isn't hard to do in python, but it is a bit of a pain to do with the JSON/COPY commands for the denormalized data.
    Since our goal is for the normalized/denormalized versions of the data to match exactly,
    we're not going to escape the strings for the normalized data.

    >>> remove_nulls('\x00')
    '\\x00'
    >>> remove_nulls('hello\x00 world')
    'hello\\x00 world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','\\x00')


def batch(iterable, n=1):
    '''
    Group an iterable into batches of size n.

    >>> list(batch([1,2,3,4,5], 2))
    [[1, 2], [3, 4], [5]]
    >>> list(batch([1,2,3,4,5,6], 2))
    [[1, 2], [3, 4], [5, 6]]
    >>> list(batch([1,2,3,4,5], 3))
    [[1, 2, 3], [4, 5]]
    >>> list(batch([1,2,3,4,5,6], 3))
    [[1, 2, 3], [4, 5, 6]]
    '''
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def _bulk_insert_sql(table, rows):
    '''
    This function generates the SQL for a bulk insert.
    It is not intended to be called directly,
    but is a helper for the bulk_insert function.
    ... (docstring truncated for brevity) ...
    '''
    if not rows:
        raise ValueError('Must be at least one dictionary in the rows variable')
    else:
        keys = set(rows[0].keys())
        for row in rows:
            if set(row.keys()) != keys:
                raise ValueError('All dictionaries must contain the same keys')
    sql = (f'''
    INSERT INTO {table}
        (''' + ','.join(keys) + ''')
        VALUES
        ''' + ','.join([ '('+','.join([f':{key}{i}' for key in keys])+')' for i in range(len(rows))]) + '''
        ON CONFLICT DO NOTHING
        ''')

    binds = { key+str(i):value for i,row in enumerate(rows) for key,value in row.items() }
    return (' '.join(sql.split()), binds)


def bulk_insert(connection, table, rows):
    '''
    Insert the data contained in the `rows` variable into the `table` relation.
    '''
    if len(rows) == 0:
        return
    sql, binds = _bulk_insert_sql(table, rows)
    connection.execute(sqlalchemy.sql.text(sql), binds)

################################################################################
# main functions
################################################################################

def insert_tweets(connection, tweets, batch_size=1000):
    '''
    Efficiently inserts many tweets into the database.
    '''
    for i, tweet_batch in enumerate(batch(tweets, batch_size)):
        print(datetime.datetime.now(), 'insert_tweets i=', i)
        _insert_tweets(connection, tweet_batch)


def _insert_tweets(connection, input_tweets):
    '''
    Inserts a single batch of tweets into the database.
    '''
    users = []
    tweets = []
    users_unhydrated_from_tweets = []
    users_unhydrated_from_mentions = []
    tweet_mentions = []
    tweet_tags = []
    tweet_media = []
    tweet_urls = []

    ########################################
    # STEP 1: generate the lists
    ########################################
    for tweet in input_tweets:
        # --- USERS ---
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

        # --- TWEETS ---
        # (geo parsing and text extraction same as before)
        try:
            geo_coords = tweet['geo']['coordinates']
            geo_coords = f"{geo_coords[0]} {geo_coords[1]}"
            geo_str = 'POINT'
        except TypeError:
            # (polygon parsing omitted for brevity)
            geo_str = None
            geo_coords = None

        try:
            text = tweet['extended_tweet']['full_text']
        except:
            text = tweet['text']

        country_code = None
        try:
            country_code = tweet['place']['country_code'].lower()
        except Exception:
            pass

        state_code = None
        if country_code == 'us':
            sc = tweet['place']['full_name'].split(',')[-1].strip().lower()
            state_code = sc if len(sc) <= 2 else None

        place_name = None
        try:
            place_name = tweet['place']['full_name']
        except Exception:
            pass

        if tweet.get('in_reply_to_user_id', None) is not None:
            users_unhydrated_from_tweets.append({
                'id_users': tweet['in_reply_to_user_id'],
                'screen_name': remove_nulls(tweet['in_reply_to_screen_name']),
            })

        tweets.append({
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet['created_at'],
            'in_reply_to_status_id': tweet.get('in_reply_to_status_id', None),
            'in_reply_to_user_id': tweet.get('in_reply_to_user_id', None),
            'quoted_status_id': tweet.get('quoted_status_id', None),
            'geo_coords': geo_coords,
            'geo_str': geo_str,
            'retweet_count': tweet.get('retweet_count', None),
            'quote_count': tweet.get('quote_count', None),
            'favorite_count': tweet.get('favorite_count', None),
            'withheld_copyright': tweet.get('withheld_copyright', None),
            'withheld_in_countries': tweet.get('withheld_in_countries', None),
            'place_name': place_name,
            'country_code': country_code,
            'state_code': state_code,
            'lang': tweet.get('lang'),
            'text': remove_nulls(text),
            'source': remove_nulls(tweet.get('source', None)),
        })

        # --- TWEET_URLS ---
        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']
        for url_obj in urls:
            tweet_urls.append({
                'id_tweets': tweet['id'],
                'url': remove_nulls(url_obj['expanded_url']),
            })

        # --- TWEET_MENTIONS ---
        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']
        for mention in mentions:
            users_unhydrated_from_mentions.append({
                'id_users': mention['id'],
                'name': remove_nulls(mention['name']),
                'screen_name': remove_nulls(mention['screen_name']),
            })
            tweet_mentions.append({
                'id_tweets': tweet['id'],
                'id_users': mention['id'],
            })

        # --- TWEET_TAGS ---
        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags']
            cashtags = tweet['extended_tweet']['entities']['symbols']
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']
        tags = ['#' + h['text'] for h in hashtags] + ['$' + c['text'] for c in cashtags]
        for tag in tags:
            tweet_tags.append({
                'id_tweets': tweet['id'],
                'tag': remove_nulls(tag),
            })

        # --- TWEET_MEDIA ---
        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []
        for medium in media:
            tweet_media.append({
                'id_tweets': tweet['id'],
                'url': remove_nulls(medium['media_url']),
                'type': medium['type'],
            })

    ########################################
    # STEP 2: perform the actual SQL inserts
    ########################################
    with connection.begin() as trans:
        bulk_insert(connection, 'users', users)
        bulk_insert(connection, 'users', users_unhydrated_from_tweets)
        bulk_insert(connection, 'users', users_unhydrated_from_mentions)
        bulk_insert(connection, 'tweet_mentions', tweet_mentions)
        bulk_insert(connection, 'tweet_tags', tweet_tags)
        bulk_insert(connection, 'tweet_media', tweet_media)
        bulk_insert(connection, 'tweet_urls', tweet_urls)

        # Tweets require special handling for geo
        sql = sqlalchemy.sql.text(
            'INSERT INTO tweets'
            ' (id_tweets,id_users,created_at,in_reply_to_status_id,in_reply_to_user_id,quoted_status_id,geo,retweet_count,quote_count,favorite_count,withheld_copyright,withheld_in_countries,place_name,country_code,state_code,lang,text,source)'
            ' VALUES ' +
            ','.join([f"(:id_tweets{i},:id_users{i},:created_at{i},:in_reply_to_status_id{i},:in_reply_to_user_id{i},:quoted_status_id{i},ST_GeomFromText(:geo_str{i} || '(' || :geo_coords{i} || ')'),:retweet_count{i},:quote_count{i},:favorite_count{i},:withheld_copyright{i},:withheld_in_countries{i},:place_name{i},:country_code{i},:state_code{i},:lang{i},:text{i},:source{i})" for i in range(len(tweets))]) +
            ' ON CONFLICT DO NOTHING'
        )
        connection.execute(sql, {key+str(i): value for i, tweet in enumerate(tweets) for key, value in tweet.items()})

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--batch_size', type=int, default=1000)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets_batch.py --inputs ' + ' '.join(args.inputs),
    })
    connection = engine.connect()

    with connection.begin() as trans:
        for filename in sorted(args.inputs, reverse=True):
            with zipfile.ZipFile(filename, 'r') as archive:
                print(datetime.datetime.now(), filename)
                for subfilename in sorted(archive.namelist(), reverse=True):
                    with io.TextIOWrapper(archive.open(subfilename)) as f:
                        tweets = [json.loads(line) for line in f]
                        insert_tweets(connection, tweets, args.batch_size)

