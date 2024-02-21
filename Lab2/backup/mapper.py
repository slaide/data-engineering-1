#!/home/ubuntu/a2t22/env/bin/python

import sys
import orjson as json
import regex as re
import time
import typing as tp

def main():
    pronouns="han,hon,hen,den,det,denna,denne".split(",")
    # match:
    # - case insensitive
    # - match any of the pronouns
    # - if it starts at a word boundary (\b)
    # - if it ends in [punctuation followed by] word boundary or end of string
    
    # match start of line OR lookback(any unicode letter or mark)
    pre_wb=r"(?:^|(?<![\p{L}\p{M}]))"
    # match end of line OR lookforward(any unicode letter or mark)
    post_wb=r"(?:$|(?![\p{L}\p{M}]))"

    # re.IGNORECASE ignores the case
    # re.MULTILINE means that ^$ match the start and end of line, not just start and end of string
    any_pronoun_re=re.compile(f'{pre_wb}({"|".join(pronouns)}){post_wb}',re.IGNORECASE)

    def get_matches(tweet_text:str)->tp.Optional[tp.List[str]]:
        tweet_text=tweet_text.lstrip().rstrip()

        if len(tweet_text)==0:
            return None

        t=json.loads(tweet_text)
        
        if 'retweeted_status' in t:
            return None

        text=t['text']
        
        matches = any_pronoun_re.findall(text)
        # deduplicate (same pronoun multiple times in same tweet only counts as 1)
        # also, convert to lowercase (matches are case insensitive, i.e. match may be upper/lowercase)
        matches=set((p.lower() for p in matches))
        
        return matches
        
    # input comes from STDIN (standard input)
    # iterate over stdin, line by line (implicitely)
    for line in sys.stdin:
        res=get_matches(line)

        if res is None:
            continue

        print(f"tweets\t{1}")
            
        for pronoun in res:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            #
            # tab-delimited; the trivial word count is 1
            print(f'{pronoun}\t{1}')

main()
