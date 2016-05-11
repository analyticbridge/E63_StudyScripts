import os
import math
from pyspark import SparkContext
from pyspark import SparkConf
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.evaluation import RegressionMetrics


#load the Data file in csv with header as first line 
autoData=sc.textFile ("/user/cloudera/hw11/SmallCarData.csv")
autoData.cache()
header=autoData.first()
autoData=autoData.filter(lambda line:line != header)
autoData = autoData.map(lambda line: line.split(","))

df = autoData.map(lambda line: Row(dis = line[3], hp = line[4])).toDF()
Acceleration,Cylinders,Displacement,Horsepower,Manufacturer,Model,Model_Year,MPG,Origin,Weight
df1=autoData.map(lambda line: Row(acc = line[1],cyl= line[2],dis=line[3], \
hp=line[4],mauf = line[5],year=line[7],orig=line[9],wt=line[10])).toDF()
>>> df1.show(3)
+----+---+---+---+-------------+-------+----+----+
| acc|cyl|dis| hp|         mauf|   orig|  wt|year|
+----+---+---+---+-------------+-------+----+----+
|  12|  8|307|130|chevrolet    |USA    |3504|  70|
|11.5|  8|350|165|buick        |USA    |3693|  70|
|  11|  8|318|150|plymouth     |USA    |3436|  70|
+----+---+---+---+-------------+-------+----+----+
only showing top 3 rows

#df.show(5)
#Liner regress will begin now 
temp = df.map(lambda line:LabeledPoint(line[0],[line[1:]]))
features = df.map(lambda row: row[1:])
standardizer = StandardScaler()
model = standardizer.fit(features)
features_transform = model.transform(features)
#features_transform.take(5)
##put label and featire together
lab = df.map(lambda row: row[0])
transformedData = lab.zip(features_transform)
transformedData.take(5)
transformedData = transformedData.map(lambda row: LabeledPoint(row[0],[row[1]]))
transformedData.take(5)
trainingData, testingData = transformedData.randomSplit([.9,.1],seed=123)
linearModel = LinearRegressionWithSGD.train(trainingData,10,.1)
#pull the coff
linearModel.weights
testingData.take(10)
#checking with model
linearModel.predict([1.49297445326,3.52055958053,1.73535287287])
from pyspark.mllib.evaluation import RegressionMetrics
prediObserRDDin = trainingData.map(lambda row: (float(linearModel.predict(row.features[0])),row.label))

metrics = RegressionMetrics(prediObserRDDin)
>>> metrics.r2
0.8157592275325074


>>> prediObserRDDout = testingData.map(lambda row: (float(linearModel.predict(row.features[0])),row.label))
>>> metrics = RegressionMetrics(prediObserRDDout)
>>> metrics.rootMeanSquaredError
49.086784219812237

>>> prediObserRDDin.take(3)
[(247.41296620034123, 307.0), (314.02414940812542, 350.0), (285.47649946193218, 318.0)]
>>> prediObserRDDout.take(3)
[(218.86531625414804, 133.0), (323.54003272352315, 383.0), (285.47649946193218, 318.0)]
>>> prediObserRDDin.saveAsTextFile("/user/cloudera/hw11/prediObserRDDin")
>>> prediObserRDDout.saveAsTextFile ("/user/cloudera/hw11/prediObserRDDout")

[cloudera@quickstart ~]$ hadoop fs -ls /user/cloudera/hw11/prediObserRDDout
Found 3 items
-rw-r--r--   1 cloudera supergroup          0 2016-04-22 09:01 /user/cloudera/hw11/prediObserRDDout/_SUCCES                 S
-rw-r--r--   1 cloudera supergroup         84 2016-04-22 09:01 /user/cloudera/hw11/prediObserRDDout/part-00                 000
-rw-r--r--   1 cloudera supergroup        195 2016-04-22 09:01 /user/cloudera/hw11/prediObserRDDout/part-00  