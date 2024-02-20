# delete output directory so that it can be created to put the output in
hdfs dfs -rm -r -f tweets-output
# run hadoop
cd # to go HOME where the hadoop folder is
time hadoop jar ./hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming*.jar \
 -files /home/ubuntu/tweets/code/mapper.py,/home/ubuntu/tweets/code/reducer.py \
 -mapper /home/ubuntu/tweets/code/mapper.py \
 -reducer /home/ubuntu/tweets/code/reducer.py \
 -input /user/ubuntu/tweets/t*.txt -output /user/ubuntu/tweets-output
# print output
hdfs dfs -cat tweets-output/part*
