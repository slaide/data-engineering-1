from pathlib import Path
import sys
import pymongo as mdb
from tqdm import tqdm
import gc
import polars as pl

assert mdb.has_c()

pronouns="han,hon,hen,den,det,denna,denne".split(",")

# delete and ingest data again?
reload_db=False
if "reload" in sys.argv:
    reload_db=True

tweet_root=Path("/home/ubuntu/tweets/data")
tweet_files=list(tweet_root.glob("t*.txt"))

mdb_client=mdb.MongoClient("mongodb://localhost:27017")
tweets_db=mdb_client.twitter.tweets

def re_match(word:str)->str:
    # match start of line OR lookback(any unicode letter or mark)
    pre_wb=r"(?:^|(?<![\p{L}\p{M}]))"
    # match end of line OR lookforward(any unicode letter or mark)
    post_wb=r"(?:$|(?![\p{L}\p{M}]))"

    re_str=f"{pre_wb}{word}{post_wb}"
    return re_str

if reload_db:
    dropped_=tweets_db.drop()
    print("ingesting tweets")

    for file in tqdm(tweet_files,desc="files"):
        # read the whole file at once
        tweets=pl.read_ndjson(file,schema={"text":pl.Utf8,"retweeted_status":pl.Struct({"id":pl.Int64})})

        # filter out all tweets that have a retweeted_status.id field containing some non-null value
        # which is only the case on retweets
        tweets=tweets.filter(
            pl.col("retweeted_status").is_null()
        ).drop("retweeted_status")

        # insert all tweets from this file at once (named=True returns a list of dictionaries)
        # ordered=False may parallelize the ingestion
        tweets_db.insert_many(tweets.rows(named=True),ordered=False)

        # make sure the dataframe is deleted from memory
        del tweets
        gc.collect()


num_tweets=tweets_db.count_documents({})
print(f"total number of tweets: {num_tweets}")
    
for pronoun in sorted(pronouns):    
    re_str=re_match(pronoun)
    
    num_results=tweets_db.count_documents(
        # filter on the text field
        {
            "text":{
                # match regex above
                "$regex":re_str,
                # i: match regex case insensitive
                "$options":"i"
            }
        },
    )
    
    print(f"{pronoun} {num_results} ({(num_results/num_tweets*100):.2f}% of total)")
