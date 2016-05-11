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

def extract_features_dt(record):
    return np.array(map(float, record[2:14]))
    
    num_vec = np.array([float(field) for field in record[10:14]])
    return np.concatenate((cat_vec, num_vec))

# function to extract the label from the last column

def extract_label(record):
    return float(record[-1])

	
records=records.filter(lambda x: "Displacement" not in x) # get that header out 

data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
first_point_dt = data_dt.first()

print "Decision Tree feature vector: " + str(first_point_dt.features)
print "Decision Tree feature vector length: " + str(len(first_point_dt.features))

dt_model = DecisionTree.trainRegressor(data_dt, {})
preds = dt_model.predict(data_dt.map(lambda p: p.features))

actual = data.map(lambda p: p.label)
true_vs_predicted_dt = actual.zip(preds)

print "Decision Tree predictions: " + str(true_vs_predicted_dt.take(5))
print "Decision Tree depth: " + str(dt_model.depth())
print "Decision Tree number of nodes: " + str(dt_model.numNodes())

//	Matrix calculated for decision tree.

# compute performance metrics for decision tree model
mse_dt = true_vs_predicted_dt.map(lambda (t, p): squared_error(t, p)).mean()
mae_dt = true_vs_predicted_dt.map(lambda (t, p): abs_error(t, p)).mean()
rmsle_dt = np.sqrt(true_vs_predicted_dt.map(lambda (t, p): squared_log_error(t, p)).mean())


print "Decision Tree - Mean Squared Error: %2.4f" % mse_dt
print "Decision Tree - Mean Absolute Error: %2.4f" % mae_dt
print "Decision Tree - Root Mean Squared Log Error: %2.4f" % rmsle_dt

