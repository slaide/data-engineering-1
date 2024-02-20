import sys
from pathlib import Path

if len(sys.argv)<3:
    print("must provide paths to python and mongodb diff files!")
    sys.exit(-1)

python_file=Path(sys.argv[1])
assert python_file.exists(), python_file
mongodb_file=Path(sys.argv[2])
assert mongodb_file.exists(), mongodb_file

python_ids=set()
with python_file.open("r") as f:
    for l in f:
        try:
            v=int(l.rstrip())
        except Exception as e:
            continue

        python_ids.add(v)
        
mongodb_ids=set()
with mongodb_file.open("r") as f:
    for l in f:
        try:
            v=int(l.rstrip())
        except Exception as e:
            continue

        mongodb_ids.add(v)

diff=mongodb_ids.difference(python_ids)
print(len(diff))
for i in diff:
    print(i)
