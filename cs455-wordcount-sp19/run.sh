#hdfs dfs -mkdir /test
#hdfs dfs -mkdir /test/books
#hdfs dfs -put *.txt /test/books
hadoop jar build/libs/cs455-wordcount-sp19.jar cs455.hadoop.wordcount.WordCountJob /test/books /test/wordcount2-out
