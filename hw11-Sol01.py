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

 
#load the Data file in csv with header as first line 
autoData=sc.textFile ("/user/cloudera/hw11/SmallCarData.csv")
autoData.cache()
header=autoData.first()
autoData=autoData.filter(lambda line:line != header)
autoData = autoData.map(lambda line: line.split(","))

df = autoData.map(lambda line: Row(dis = line[3], hp = line[4])).toDF()

#df= df.map ( lambda x: float(x) for x in line)
features = df.map(lambda row: row[1:])
features.take(1)
label = df.map(lambda line:LabeledPoint(line[0],[line[1:]]))
label.take(3)
standardizer = StandardScaler()
model = standardizer.fit(features)
features_transform = model.transform(features)
features_transform.take(5)

###put together##
lab = df.map(lambda row: row[0])
lab.take(3)
#chg below lab to label 

transformedData = label.zip(features_transform)
transformedData.take(5)

##split the Datset 

trainingData, testingData = transformedData.randomSplit([.9,.1],seed=123)

trainingData.take(3)

testingData.take(3)
trainingData.count()
testingData.count()

linear_model = LinearRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)

data = trainingData.map(lambda r:LabeledPoint(extract_label(r),extract_features(r)))

linear_model = LinearRegressionWithSGD.train(data, iterations=100,step=0.01, intercept=False)

linearModel = LinearRegressionWithSGD.train(df,100,.2)

linear_model = LinearRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)


