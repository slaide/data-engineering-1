#!/usr/bin/env pypy3

import sys
import regex as re
import time
import typing as tp
from pathlib import Path
from tqdm import tqdm
import polars as pl
from collections import Counter
import orjson as json

# let it be known that i tried to parallelize this, but it just made this program slower
# probably because these nodes are super low on resources, by any modern standards

def main():
	pronouns="han,hon,hen,den,det,denna,denne".split(",")
	# match:
	# - case insensitive
	# - match any of the pronouns
	# - if it starts at a word boundary (\b)
	# - if it ends in [punctuation followed by] word boundary or end of string
	escaped_pronouns=[re.escape(p) for p in pronouns]

	# match start of line OR lookback(any unicode letter or mark)
	pre_wb=r"(?:^|(?<![\p{L}\p{M}]))"
	# match end of line OR lookforward(any unicode letter or mark)
	post_wb=r"(?:$|(?![\p{L}\p{M}]))"

	all_pronouns_re=re.compile(f'{pre_wb}({"|".join(pronouns)}){post_wb}',re.IGNORECASE)

	root=Path("/home/ubuntu/tweets/data")
	tweet_files=list(root.glob("t*.txt"))

	total_tweets=0

	counts=Counter()

	for file in tqdm(tweet_files,desc="files",total=len(tweet_files),disable=False):
		lc=Counter()
		with file.open("r") as file:
			for line in file:
				line=line.rstrip().lstrip()

				if len(line)==0:
					continue

				tweet=json.loads(line)

				if "retweeted_status" in tweet:
					continue

				total_tweets+=1
				
				matches=all_pronouns_re.findall(tweet["text"])
				matches=set((p.lower() for p in matches))

				lc.update(matches)

		counts.update(lc)

	print(f"{total_tweets = }")
	for p,c in sorted(counts.items(),key=lambda i:i[0]):
		print(f"{p}\t{c}\t({(c/total_tweets*100):6.2f}% of total)")

main()
