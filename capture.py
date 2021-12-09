from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd
import re


print('Capturing')
sc = SparkContext("local[2]", "NetworkWordCount")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
sql_context=SQLContext(sc)
print("Connecting")
# print(lines)

def clean(tweet):
	tweet = re.sub('(\s)?@\w+','',tweet)
	tweet = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+','',tweet)
	tweet = tweet.strip()
	print(tweet)

def readLines(rdd):
	rdd.foreach(print)
	#if not rdd.isEmpty():
	'''
		df = spark.read.json(rdd)
		#df.show()
		
		print('Started the Process')
		data = []
		for i in range(len(df.columns)):
			data.append(df.collect()[0][i])
		rdd = sc.parallelize(data)
		new_df = rdd.toDF()
		#new_df.show()
		for i in range(len(df.columns)):
			tweets = df.collect()[1][i]
		print(tweets)
	'''
		
try:
	lines = ssc.socketTextStream("localhost", 6100)
	print('Reading RDDs')
	lines.foreachRDD(lambda rdd: readLines(rdd))
	print('Exiting')
	ssc.start()
	ssc.awaitTermination()
	'''
	#rdd = sc.textFile()
	df = spark.read.csv('./sentiment/new.csv')
	#df.show()
	for i in range(20):#df.count()):
		tweet = df.collect()[i][1]
		tweet = tweet.strip()
		tweet = re.sub('(\s)?@\w+','',tweet)
		tweet = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+','',tweet)
		print(tweet)
	'''

except Exception as e:
	print(e)
'''
ssc.start()
ssc.awaitTermination()
print('await')
ssc.stop()
print('stop')
'''
