# run using spark-submit cmd , data file path is hard coded for HW purpose only 
#load the modules 
import os
import math 
from pyspark.mllib.linalg import Vectors
import org.apache.spark.mllib.linalg.{Vector, Vectors}
###
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
###

# load the csv files into RDD
autoData=sc.textFile("/user/cloudera/hw11/SmallCarData.csv")
autoData.cache()

#Remove the forst line (contain Header)
dataLines=autoData.filter(lambda x: "Displacement" not in x)
dataLines.count()

 dataColumn = dataLines.map(lambda l: l.split(","))
 ##didnot dataLr =dataColumn.map(lambda e:( e[3],e[4]))
 dataLrf =dataColumn.map(lambda e:( float (e[3]),float (e[4])))

 
def transformToNumeric(inputStr):
	attlist=inputStr.split(",")
	

values = Vectors.dense(float(attList[0]),float(attlist[1])
	return values	
						

#keep only Cyl,Displacement
autoVectors=dataLines.map(transformToNumeric)		
autoVectors.collect()

#perform analysis
from pyspark.mllib.stat import Statistics
from pyspark.sql import SQLContext(sc)


						
autostats=Statistics.colStats(autoVectors)
autosatats.mean()
autosatats.Variance()						
autosatats.min()
autosatats.max()
Statistics.corr(autoVector)


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def transformToLabelPoint(inStr):
lp =(float(inStr[0]),vectors.dense(inStr[1]))
return lp


autolp = autovectors.map(transformToLabledPoint)
autodf = sqlContext.createDataFrame(autoLp,["label","features")
autodf.select("label","features").show(10)

#SPLIT 
(trainningData,testdData)= autoDF.randomSplit([0.9,0.1]
trainningData.count()

>>> (trainningData,testdData)= dataLr.randomSplit([0.9,0.1])
>>> trainningData.count()
89
>>> testdData.count()
11

from pyspark.ml.regression import LineraRegression
lr =LinearRegression(maxItr=10)
lrModel=LinearRegression.fit(trainningData)
print("Coff:" +str (lrModel.coefficient))
print("inter:" + str(lrModel.intercept))
