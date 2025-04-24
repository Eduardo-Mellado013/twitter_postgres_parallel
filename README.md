# Parallel Twitter in Postgres

|     | sequential | parallel |
| --- | ---------- | -------- |
| normalized (unbatched) | ![](https://github.com/Eduardo-Mellado013/twitter_postgres_parallel/workflows/tests_normalized_sequential/badge.svg) | ![](https://github.com/Eduardo-Mellado013/twitter_postgres_parallel/workflows/tests_normalized_parallel/badge.svg) |
| normalized (batched) | ![](https://github.com/Eduardo-Mellado013/twitter_postgres_parallel/workflows/tests_normalizedbatch_sequential/badge.svg) |  ![](https://github.com/Eduardo-Mellado013/twitter_postgres_parallel/workflows/tests_normalizedbatch_parallel/badge.svg) |
| denormalized | ![](https://github.com/Eduardo-Mellado013/twitter_postgres_parallel/workflows/tests_denormalized_sequential/badge.svg) | ![](https://github.com/Eduardo-Mellado013/twitter_postgres_parallel/workflows/tests_denormalized_parallel/badge.svg) |

In this assignment, you will learn how to load data into postgres much faster using two techniques:
1. batch loading (i.e. running the INSERT command on more than one row at a time)
1. and parallel loading.

You will also get practice doing the type of medium sized refactor on code that you didn't write that is common in industry.

<img src=refactor.jpg width=300px />

## Tasks

### Setup

1. Fork this repo
1. Enable github actions on your fork
1. Clone the fork onto the lambda server
1. Modify the `README.md` file so that all the test case images point to your repo
1. Modify the `docker-compose.yml` to specify valid ports for each of the postgres services
    1. recall that ports must be >1024 and not in use by any other user on the system
    1. verify that you have modified the file correctly by running
       ```
       $ docker-compose up
       ```
       with no errors

### The Data

In this project, you will be using more data than in the last homework, but still only a small subset of the full twitter dataset.
The data is located in the `data` folder.
Familiarize yourself with the data by running the commands
```
$ ls data
$ du -h data
$ for file in data/*; do echo "$file" $(unzip -p "$file" | wc -l); done
```

### Sequential Data Loading

Notice in the `docker-compose.yml` file there are now three services instead of two.
The new service `normalized_batch` contains almost the same normalized database schema as the `normalized` service.
Check the difference by running
```
$ diff services/pg_normalized/schema.sql services/pg_normalized_batch/schema.sql -u
--- services/pg_normalized/schema.sql	2023-03-31 09:17:54.452468311 -0700
+++ services/pg_normalized_batch/schema.sql	2023-03-31 09:17:54.452468311 -0700
@@ -30,7 +30,7 @@
     location TEXT,
     description TEXT,
     withheld_in_countries VARCHAR(2)[],
-    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
+    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
 );

 /*
@@ -55,8 +55,8 @@
     lang TEXT,
     place_name TEXT,
     geo geometry,
-    FOREIGN KEY (id_users) REFERENCES users(id_users),
-    FOREIGN KEY (in_reply_to_user_id) REFERENCES users(id_users)
+    FOREIGN KEY (id_users) REFERENCES users(id_users) DEFERRABLE INITIALLY DEFERRED,
+    FOREIGN KEY (in_reply_to_user_id) REFERENCES users(id_users) DEFERRABLE INITIALLY DEFERRED

     -- NOTE:
     -- We do not have the following foreign keys because they would require us
@@ -71,8 +71,8 @@
     id_tweets BIGINT,
     id_urls BIGINT,
     PRIMARY KEY (id_tweets, id_urls),
-    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets),
-    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
+    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets) DEFERRABLE INITIALLY DEFERRED,
+    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED
 );


@@ -80,8 +80,8 @@
     id_tweets BIGINT,
     id_users BIGINT,
     PRIMARY KEY (id_tweets, id_users),
-    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets),
-    FOREIGN KEY (id_users) REFERENCES users(id_users)
+    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets) DEFERRABLE INITIALLY DEFERRED,
+    FOREIGN KEY (id_users) REFERENCES users(id_users) DEFERRABLE INITIALLY DEFERRED
 );
 CREATE INDEX tweet_mentions_index ON tweet_mentions(id_users);

@@ -89,7 +89,7 @@
     id_tweets BIGINT,
     tag TEXT,
     PRIMARY KEY (id_tweets, tag),
-    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets)
+    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets) DEFERRABLE INITIALLY DEFERRED
 );
 COMMENT ON TABLE tweet_tags IS 'This table links both hashtags and cashtags';
 CREATE INDEX tweet_tags_index ON tweet_tags(id_tweets);
@@ -100,8 +100,8 @@
     id_urls BIGINT,
     type TEXT,
     PRIMARY KEY (id_tweets, id_urls),
-    FOREIGN KEY (id_urls) REFERENCES urls(id_urls),
-    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets)
+    FOREIGN KEY (id_urls) REFERENCES urls(id_urls) DEFERRABLE INITIALLY DEFERRED,
+    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets) DEFERRABLE INITIALLY DEFERRED
 );

 /*
```
You should see that the only differences between these schemas is that the batch version uses the `DEFERRABLE INITIALLY DEFERRED` line.
The file `load_tweets_batch.py` inserts 1000 tweets at a time in a single INSERT statement.
This causes consistency errors if the UNIQUE/FOREIGN KEY constraint checks are not deferred until the end of the transaction.
The resulting code is much more complicated than the code you wrote for your `load_tweets.py` in the last assignment, so I am not making you write it.
Instead, I am providing it for you.
You should see that the test cases for `test_normalizedbatch_sequential` are already passing.

#### Verifying Correctness

Your first task is to make the other two sequential tests pass.
Do this by:
1. Copying the `load_tweets.py` file from your `twitter_postgres` homework into this repo.
    (If you couldn't complete this part of the assignment, for whatever reason, than let me know and I'll give you a working copy.)
2. Modify the `load_tweets_sequential.sh` file to correctly load the tweets into the `pg_normalized` and `pg_denormalized` databases.
    You should be able to use the same lines of code as you used in the `load_tweets.sh` file from the previous assignment.
Once you've done those two steps, verify that the test cases pass by uploading to github and getting green badges.

#### Measuring Runtimes

Once you've verified that the test cases pass, on the lambda server, you should run the following commands to load the data.
```
$ docker-compose down
$ docker volume prune
$ docker-compose up -d
$ sh load_tweets_sequential.sh
```
The `load_tweets_sequential.sh` file reports the runtime of loading data into each of the three databases.
Record the runtime in the table in the Submission section below.
You should notice that batching significantly improves insertion performance speed,
but the denormalized database insertion is still the fastest.

> **NOTE:**
> The `time` command outputs 3 times:
>
> 1. The `elapsed` time (also called wall-clock time) is the actual amount of time that passes on the system clock between the program's start and end.
>    This is what should be recorded in the table above.
>
> 1. The `user` time is the total amount of CPU time used by the program.
>    This can be different than wall-clock time for 2 reasons:
>
>    1. If the process uses multiple CPUs, then all of the concurrent CPU time is added together.
>       For example, if a process uses 8 CPUS, then the `user` time could be up to 8 times higher than the actual wall-clock time.
>       (Your sequential process in this section is single threaded, so this won't be applicable; but this will be applicable for the parallel process in the next section.)
>
>    1. If the command has to wait on an external resource (e.g. disk/network IO),
>       then this waiting time is not included.
>       (Your python processes will have to wait on the postgres server,
>       and the postgres server's processing time is not included in the `user` time because it is a different process.
>       In general, the postgres server could be running on an entirely different machine.)
>
> 1. The `system` time is the total amount of CPU time used by the Linux kernel when managing this process.
>    For the vast majority of applications, this will be a very small amount.

### Parallel Data Loading

There are 10 files in `/data` folder of this repo.
If we process each file in parallel, then we should get a theoretical 10x speed up.
The file `load_tweets_parallel.sh` will insert the data in parallel and if you implement it correctly you will observe this speedup.
There are several changes that you'll have to make to your code to get this to work.

#### Denormalized Data

Currently, there is no code in the `load_tweets_parallel.sh` file for loading the denormalized data.
Your first task is to use the GNU `parallel` program to load this data.

Complete the following steps:

1. Write a POSIX script `load_denormalized.sh` that takes a single parameter as input that represents a data file.
   The script should then load this file into the database using the same technique as in the `load_tweets_sequential.sh` file for the denormalized database.
   In particular, you know you've implemented this file correctly if the following bash code correctly loads the database.
   ```
   for file in data/*; do
       sh load_denormalized.sh $file
   done
   ```

2. Call the `load_denormalized.sh` file using the `parallel` program from within the `load_tweets_parallel.sh` script.

    The `parallel` program takes a single paramater as input which is the command that it will execute.
    For each line that it receives in stdin, it will pass that line as an argument to the input command.
    All of these commands will be run in parallel,
    and the `parallel` program will terminate once all of the individual commands terminate.

    My solution looks like
    ```
    time echo "$files" | parallel ./load_denormalized.sh
    ```
    Notice that I also use the `time` command to time the insertion operation.
    One of the advantages of using the `parallel` command over the `&` operator we used previously is that it is easier to time your parallel computations.

You know you've completed this step correctly if the `run_tests.sh` script passes (locally) and the test badge turns green (on the lambda server).

#### Normalized Data (unbatched)

Modify the `load_tweets_parallel.sh` file to load the `pg_normalized` database in parallel following the same procedure above.

Parallel loading of the unbatched data will probably "just work."
The code in the `load_tweets.py` file is structured so that you never run into deadlocks.
Unfortunately, the code is extremely slow,
so even when run in parallel it is still slower than the batched code.

#### Normalized Data (batched)

Modify the `load_tweets_parallel.sh` file to load the `pg_normalized_batch` database in parallel following the same procedure above.

Parallel loading of the batched data will fail due to deadlocks.
These deadlocks will cause some of your parallel loading processes to crash.
So all the data will not get inserted,
and you will fail the `run_tests.sh` tests.

There are two possible ways to fix this.
The most naive method is to catch the exceptions generated by the deadlocks in python and repeat the failed queries.
This will cause all of the data to be correctly inserted,
so you will pass the test cases.
Unfortunately, python will have to repeat queries so many times that the parallel code will be significantly slower than the sequential code.
My code took several hours to complete!

So the best way to fix this problem is to prevent the deadlocks in the first place.

<img src=you-cant-have-a-deadlock-if-you-remove-the-locks.jpg width=600px />

In this case, the deadlocks are caused by the `UNIQUE` constraints,
and so we need to figure out how to remove those constraints.
This is unfortunately rather complicated.

The most difficult `UNIQUE` constraint to remove is the `UNIQUE` constraint on the `url` field of the `urls` table.
The `get_id_urls` function relies on this constraint, and there is no way to implement this function without the `UNIQUE` constraint.
So to delete this constraint, we will have to denormalize the representation of urls in our database.
Perform the following steps to do so:

1. Modify the `services/pg_normalized_batch/schema.sql` file by:
   1. deleting the `urls` table
   1. replacing all of the `id_urls BIGINT` columns with a `url TEXT` column
   1. deleting all foreign keys that connected the old `id_urls` columns to the `urls` table

1. Modify the `load_tweets_batch.py` file by:
   1. deleting the `get_id_urls` function
   1. modifying all of the references to the id generated by `get_id_urls` to directly store the url in the `url` field of the table

There are also several other `UNIQUE` constraints (mostly in `PRIMARY KEY`s) that need to be removed from other columns of the table.
Once you remove these constraints, this will cause downstream errors in both the SQL and Python that you will have to fix.
(But I'm not going to tell you what these errors look like in advance... you'll have to encounter them on your own.)

> **NOTE:**
> In a production database where you are responsible for the consistency of your data,
> you would never want to remove these constraints.
> In our case, however, we're not responsible for the consistency of the data.
> The data comes straight from Twitter, and so Twitter is responsible for the data consistency.
> We want to represent the data exactly how Twitter represents it "upstream",
> and so removing the UNIQUE/FOREIGN KEY constraints is reasonable.

#### Results

Once you have verified the correctness of your parallel code,
bring up a fresh instances of your containers and measure your code's runtime with the command
```
$ sh load_tweets_parallel.sh
```
Record the elapsed times in the table below.
You should notice that parallelism achieves a nearly (but not quite) 10x speedup in each case.

## Submission

Ensure that your runtimes on the lambda server are recorded below.

|                        | elapsed time (sequential) | elapsed time (parallel)   |
| -----------------------| ------------------------- | ------------------------- |
| `pg_normalized`        |         4m18.67s          |      1m28.74s             | 
| `pg_normalized_batch`  |         1m10.25s          |      0m31.29s             | 
| `pg_denormalized`      |         0m1.65s           |      0m06.80s             | 

Then upload a link to your forked github repo on sakai.

> **GRADING NOTE:**
> It is not enough to just get passing test cases for this assignment in order to get full credit.
> (It is easy to pass the test cases by just doing everything sequentially.)
> Instead, you must also implement the parallelism correctly so that the parallel runtimes above are about 10x faster than the sequential runtimes.
> (Again, they should be 10x faster because we are doing 10 files in parallel.)
