#!/usr/bin/env pypy3

from operator import itemgetter
import sys

def main():
    pronouns="han,hon,hen,den,det,denna,denne,tweets".split(",")
    word_counts={p:0 for p in pronouns}

    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        lsplit=line.split('\t')
        if len(lsplit)>2:
            lsplit=lsplit[:2]
            
        # parse the input we got from mapper.py
        word, count = lsplit

        # convert count (currently a string) to int
        try:
            count = int(count)
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            continue

        word_counts[word]+=count

    for word,count in sorted(word_counts.items(),key=lambda i:i[0]):
        print(f'{word}\t{count}\t({(count/word_counts["tweets"]*100):6.2f}% of total)')

main()
