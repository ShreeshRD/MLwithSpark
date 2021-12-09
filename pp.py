from pyspark import SparkContext
#from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
#import pyspark.pandas as ps
import re
import pyspark.sql.functions as F
import pyspark.sql.types as T

'''
#print('Capturing')
sc = SparkContext("local[2]", "NetworkWordCount")
spark = SparkSession(sc)
#ssc = StreamingContext(sc, 1)
sql_context=SQLContext(sc)
print("Connecting")
# print(lines)
'''
def clean(tweet):
	tweet = re.sub('(\s)?@\w+','',tweet)
	tweet = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+','',tweet)
	tweet = tweet.strip()
	return tweet

def readLines(rdd):
	rdd.foreach(print)
	if not rdd.isEmpty():
		rdd.map(clean)
'''	
try:
	#data = ["Project @yomama","Gutenberg’s","Alice’s","Adventures","in","Wonderland"]
	#rdd = spark.sparkContext.parallelize(data)
	#rdd.foreach(print)
	df = spark.read.csv('./sentiment/new.csv', header = True)
	#df.transform(clean)
	udf_clean = F.UserDefinedFunction(clean, T.StringType())
	df = df.withColumn('clean', udf_clean('Tweet'))
	df = df.drop('Tweet').withColumnRenamed('clean', 'Tweet')
	#df = df.dropna()
	df.show()
	#tweets = df.select('Tweet').rdd
	#sent = df.select('Sentiment').rdd
	#rdd.map(clean)
	#rdd.foreach(print)

except Exception as e:
	print(e)

ssc.start()
ssc.awaitTermination()
print('await')
ssc.stop()
print('stop')
'''
