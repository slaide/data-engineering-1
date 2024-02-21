cd ..
PY=/home/ubuntu/a2t22/env/bin/python

time cat data/tweets_*.txt | $PY code/mapper.py | sort -k1,1 | $PY code/reducer.py
