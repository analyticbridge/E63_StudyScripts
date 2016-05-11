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
from pyspark.mllib.tree import DecisionTree

autoData02=sc.textFile ("/user/cloudera/hw11/SmallCarData.csv")
autoData02.cache()
header=autoData02.first()

Data02=autoData02.filter(lambda line:line != header)

autoData02 = autoData02.map(lambda line: line.split(","))

autoData02=autoData02.filter(lambda line:line != header)
autoData02 = autoData02.map(lambda line: line.split(","))
df1=autoData02.map(lambda line: Row(acc = line[1],cyl= line[2],dis=line[3],hp=line[4], mauf = line[5],year=line[7],orig=line[9],wt=line[10])).toDF()

def get_mapping(rdd, idx):
   	 return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

	 print "Mapping of first categorical feature column: %s" % get_mapping(df1,4)
print "Mapping of first categorical feature column: %s" % get_mapping(df1,5)

mappings = [get_mapping(df1, i) for i in range(1,50)]
cat_len = sum(map(len, mappings))
num_len = len(df1.first()[4:5])

total_len = num_len + cat_len

print "Feature vector length for categorical features: %d" % cat_len 
print "Feature vector length for numerical features: %d" % num_len
print "Total feature vector length: %d" % total_len

def extract_features(record):
    cat_vec = np.zeros(cat_len)
    i = 0
    step = 0
    for field in record[2:9]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)
    
    num_vec = np.array([float(field) for field in record[10:14]])
    return np.concatenate((cat_vec, num_vec))

# function to extract the label from the last column
def extract_label(record):
    return float(record[-1])

	
records=records.filter(lambda x: "Displacement" not in x) # get that header out 
data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r)))
first_point = data.first()

linear_model = LinearRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)
	
	true_vs_predicted = data.map(lambda p: (p.label, linear_model.predict(p.features)))

	#PERFORMANCE MATRIX
	
def squared_error(actual, pred):
    return (pred - actual)**2

def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2

mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean()
rmsle = np.sqrt(true_vs_predicted.map(lambda (t, p): squared_log_error(t, p)).mean())
