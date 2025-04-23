CREATE EXTENSION postgis;

\set ON_ERROR_STOP on

BEGIN;

-- Drop the old normalized-url table entirely
DROP TABLE IF EXISTS urls;

-- USERS: no UNIQUE/PK, store URL text directly
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id_users BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    url TEXT,
    friends_count INTEGER,
    listed_count INTEGER,
    favourites_count INTEGER,
    statuses_count INTEGER,
    protected BOOLEAN,
    verified BOOLEAN,
    screen_name TEXT,
    name TEXT,
    location TEXT,
    description TEXT,
    withheld_in_countries VARCHAR(2)[]
);
CREATE INDEX IF NOT EXISTS users_id_idx ON users(id_users);

-- TWEETS: keep the primary key, but remove FKs to users
DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets (
    id_tweets BIGINT PRIMARY KEY,
    id_users BIGINT,
    created_at TIMESTAMPTZ,
    in_reply_to_status_id BIGINT,
    in_reply_to_user_id BIGINT,
    quoted_status_id BIGINT,
    retweet_count SMALLINT,
    favorite_count SMALLINT,
    quote_count SMALLINT,
    withheld_copyright BOOLEAN,
    withheld_in_countries VARCHAR(2)[],
    source TEXT,
    text TEXT,
    country_code VARCHAR(2),
    state_code VARCHAR(2),
    lang TEXT,
    place_name TEXT,
    geo geometry
    -- No foreign-key constraints on id_users or in_reply_to_user_id
);
CREATE INDEX tweets_index_geo               ON tweets USING gist(geo);
CREATE INDEX tweets_index_withheldincountries ON tweets USING gin(withheld_in_countries);

-- TWEET_URLS: denormalized URLs, no PK, no FK to users or URLs
DROP TABLE IF EXISTS tweet_urls;
CREATE TABLE tweet_urls (
    id_tweets BIGINT,
    url       TEXT
);
CREATE INDEX IF NOT EXISTS tweet_urls_tweetid_idx ON tweet_urls(id_tweets);

-- TWEET_MENTIONS: no foreign keys at all
DROP TABLE IF EXISTS tweet_mentions;
CREATE TABLE tweet_mentions (
    id_tweets BIGINT,
    id_users  BIGINT
);
CREATE INDEX IF NOT EXISTS tweet_mentions_idx_t ON tweet_mentions(id_tweets);
CREATE INDEX IF NOT EXISTS tweet_mentions_idx_u ON tweet_mentions(id_users);

-- TWEET_TAGS: no PK
DROP TABLE IF EXISTS tweet_tags;
CREATE TABLE tweet_tags (
    id_tweets BIGINT,
    tag       TEXT
);
CREATE INDEX IF NOT EXISTS tweet_tags_idx_t ON tweet_tags(id_tweets);

-- TWEET_MEDIA: no PK, denormalized URLs
DROP TABLE IF EXISTS tweet_media;
CREATE TABLE tweet_media (
    id_tweets BIGINT,
    url       TEXT,
    type      TEXT
);
CREATE INDEX IF NOT EXISTS tweet_media_idx_t ON tweet_media(id_tweets);

-- MATERIALIZED VIEWS: unchanged
DROP MATERIALIZED VIEW IF EXISTS tweet_tags_total;
CREATE MATERIALIZED VIEW tweet_tags_total AS (
    SELECT
        row_number() OVER (ORDER BY count(*) DESC) AS row,
        tag,
        count(*) AS total
    FROM tweet_tags
    GROUP BY tag
    ORDER BY total DESC
);

DROP MATERIALIZED VIEW IF EXISTS tweet_tags_cooccurrence;
CREATE MATERIALIZED VIEW tweet_tags_cooccurrence AS (
    SELECT
        t1.tag AS tag1,
        t2.tag AS tag2,
        count(*) AS total
    FROM tweet_tags t1
    JOIN tweet_tags t2 ON t1.id_tweets = t2.id_tweets
    GROUP BY t1.tag, t2.tag
    ORDER BY total DESC
);

COMMIT;

