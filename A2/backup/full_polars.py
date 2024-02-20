#!/usr/bin/env pypy3

import sys
import regex as re
import time
import typing as tp
from pathlib import Path
from tqdm import tqdm
import polars as pl
from collections import Counter

# let it be known that i tried to parallelize this, but it just made this program slower
# probably because these nodes are super low on resources, by any modern standards

def main():
    pronouns="han,hon,hen,den,det,denna,denne".split(",")
    # match:
    # - case insensitive
    # - match any of the pronouns
    # - if it starts at a word boundary (\b)
    # - if it ends in [punctuation followed by] word boundary or end of string
    
    # match start of line OR lookback(any unicode letter or mark)
    pre_wb=r"(?:^|[^\p{L}\p{M}])"
    # match end of line OR lookforward(any unicode letter or mark)
    post_wb=r"(?:$|[^\p{L}\p{M}])"
    pronoun_regexes=[(p,f"{pre_wb}{p}{post_wb}") for p in pronouns]

    root=Path("/home/ubuntu/tweets/data")
    tweet_files=list(root.glob("t*.txt"))

    total_tweets=0

    counts=Counter()

    for file in tqdm(tweet_files,desc="files",total=len(tweet_files),disable=False):
        file=str(file)
        tweets=pl.read_ndjson(file,schema={"text":pl.Utf8,"retweeted_status":pl.Struct({"id":pl.Int64})})

        tweets=tweets.filter(
            pl.col("retweeted_status").is_null()
        ).drop(
            "retweeted_status"
        ).with_columns(
            pl.col("text").str.to_lowercase().alias("text_lowercase")
        ).with_columns([
            pl.col("text_lowercase").str.contains(p_re).alias(p)
            for (p,p_re)
            in pronoun_regexes
        ])

        total_tweets+=len(tweets)

        counts.update({
            p:tweets[p].sum()
            for p
            in pronouns
        })

    print(f"{total_tweets = }")
    for p,c in sorted(counts.items(),key=lambda i:i[0]):
        print(f"{p}\t{c}\t({(c/total_tweets*100):6.2f}% of total)")

main()
