from sklearn.preprocessing import MaxAbsScaler
from sklearn.feature_extraction.text import HashingVectorizer
import pickle
from sklearn import preprocessing
from sklearn.metrics import accuracy_score
from pyspark import SparkContext
from pyspark import SQLContext
from sklearn.linear_model import PassiveAggressiveClassifier
import numpy as np
from pyspark.streaming import StreamingContext
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
import re

def my_clean(tweet):
    tweet = re.sub('(\s)?@\w+','',tweet)
    tweet = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+','',tweet)
    tweet = tweet.strip()
    return tweet

def flatten_json(x):
    flattened_json_list=json.loads(x).values()
    for dicts in flattened_json_list:
        for key in dicts:
            dicts[key]=str(dicts[key])
    return(flattened_json_list)


# DataFrame operations inside your streaming program
sc = SparkContext("local[2]", "StreamingMachineLearning")
spark_context=SQLContext(sc)
ssc = StreamingContext(sc, 5)
lines=ssc.socketTextStream("localhost", 6100)

def process(time, rdd):
    #print("========= %s =========" % str(time))
    try:
        if(rdd==[] or rdd is None or rdd==[[]]):
            return
        rdd=rdd.flatMap(lambda x: flatten_json(x))
        df = spark_context.createDataFrame(rdd, ["sentiment","tweet"])
        
        udf_clean = F.UserDefinedFunction(my_clean, T.StringType())
        df = df.withColumn('clean', udf_clean('tweet'))
        df = df.drop('tweet').withColumnRenamed('clean', 'tweet')

        X=df.select('tweet').collect()
        X=[row.tweet for row in X]
        vectorizer=HashingVectorizer(stop_words = 'english')
        x_train=vectorizer.fit_transform(X)
        #print(x_train)
        scaler=MaxAbsScaler()
        x_train=scaler.fit_transform(x_train)
        #print(x_train)
        y_train=df.select('sentiment').collect()
        y_train=np.array([row[0] for row in np.array(y_train)])

        le = preprocessing.LabelEncoder()
        y_train=le.fit_transform(y_train)


        global count
        global first_it
        clf=None

        if(first_it):
            first_it=False
            clf = PassiveAggressiveClassifier()
            #print('Inital run , accuracy cannot be calculated')
        else:
            with open('./temp', 'rb') as p:
                clf = pickle.load(p)
            preds=clf.predict(x_train)
            score=accuracy_score(y_train,preds)
            print(count," ",score)
            count+=1

        clf.partial_fit(x_train,y_train,classes=np.unique([0,1]))

        pickle.dump(clf, open('./temp', 'wb'))

    except Exception as e:
        print(e)
    
first_it=True
count=0

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
