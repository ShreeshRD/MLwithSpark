import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import udf,variance
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
sc.setLogLevel("OFF")
ssc = StreamingContext(sc, 1)
spark=SparkSession(sc)

# Create a DStream that will connect to hostname:port, like localhost:9999
data = ssc.socketTextStream("localhost", 6100)
#data.pprint()
#print("hi")

try:
	def readMyStream(rdd):
		df=spark.read.json(rdd)
		print(df)
except Exception as e:
	print(e)
#print('hello')

try:
	data.foreachRDD(lambda rdd: readyMyStream(rdd))
except Exception as e:
	print(e)

ssc.start()
time.sleep(1000)
ssc.stop(stopSparkContext=False)
