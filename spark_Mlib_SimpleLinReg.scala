package edu.hu.examples

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics //Notice
import org.apache.spark.mllib.linalg.distributed.RowMatrix //BetterLR
import org.apache.spark.mllib.feature.StandardScaler

object SimpleLinReg {
  def main(args: Array[String]) {

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("linearreg")
    val sc = new SparkContext(conf)

    // Read the input file from local dir, its a csv file without header
	val rawtrainrdd = sc.textFile("file:///home/cloudera/Documents/assign11/data2.csv")
	
	// Split data randomly into training (90%) and test (10%).
	val splits = rawtrainrdd.randomSplit(Array(0.9, 0.1), seed = 1)
	val training = splits(0).cache()
	val test = splits(1).cache()
	val testsize = test.count()
	val trainingsize = training.count()
	
	// Print out the test size and training size to show the # of data in each
	println("test size: " + testsize)
	println("training size: " + trainingsize)

	// parts(4) is horsepower, which is the target variable
	// parts(3) is displacement, which is the feature
	val parsedData = training.map { line =>
		val parts = line.split(',')
  		LabeledPoint(parts(4).toInt, Vectors.dense(parts(3).split(' ').map(_.toDouble)))
	}.cache()
	
	// Create LabeledPoint RDD for the test data set
	val parsedTestData = test.map { line =>
		val parts = line.split(',')
  		LabeledPoint(parts(4).toInt, Vectors.dense(parts(3).split(' ').map(_.toDouble)))
	}.cache()

    // Building the model
	val numIterations = 1000 
	val stepSize = 0.0001 // stepSize smaller than this gave NaN
	
	val myLR= new LinearRegressionWithSGD()
	myLR.setIntercept(true)
	myLR.optimizer.setNumIterations(numIterations).setStepSize(stepSize)
    
	val model = myLR.run(parsedData)
	
	// Evaluate model on training examples and compute training error
	val valuesAndPreds = parsedData.map { point =>
  		val prediction = model.predict(point.features)
  			(point.label, prediction)
		}
	
	// Print model parameters and compare observed vs. predicted
  	println("weights: %s, intercept: %s".format(model.weights, model.intercept))
	println("true vs. predicted: ")
	valuesAndPreds.take(10).foreach(println)
	
	// Save observed and predicted values from training data
	//valuesAndPreds.saveAsTextFile("file:///home/cloudera/Documents/assign11/trainOutput");
	
    
    // Evaluation parameters:
    val n = valuesAndPreds.count()
    // 1. Calculate SSE and MSS to be able to obtain r^2
    // SSE, sum of square errors
    val SSE = valuesAndPreds.map( vp => math.pow((vp._1 - vp._2), 2) ).
    	reduce(_+_)
    // MSS, model sum of squares
    val modelMean = valuesAndPreds.map( vp => math.pow((vp._2), 1) ).
    	reduce(_+_) / (n-1)
    val MSS = valuesAndPreds.map( vp => math.pow((modelMean - vp._2), 2) ).
    	reduce(_+_)
    	
    // 2. Calculate mean residual 
    // When linear regression is done analytically using least square method,
    // the mean residual equals zero. 
    // In ML, it won't be exactly zero, 
    // but a good model should produce something closer to zero. 		
    val meanResidual = valuesAndPreds.map( vp => math.pow((vp._1 - vp._2), 1) ).
    	reduce(_+_) / (n-1)
    	
    println("SSE, sum of square errors = " + "%6.3f".format(SSE))
   	println("MSS, model sum of squares = " + "%6.3f".format(MSS))
   	println("r^2, coeff. of det. = " + "%6.3f".format(MSS / (SSE + MSS)))
   	println("mean residual = " + "%6.3f".format(meanResidual))
   	
   	// 3. RMSE, root mean sq error
   	// This is same as the standard error of the estimate
   	println("Root Mean Squared Error = " + "%6.3f".format(math.sqrt(SSE / (n-2))))
	   	
   	// Predict using the test data and compute the test error
   	val testvaluesAndPreds = parsedTestData.map { point =>
  		val prediction = model.predict(point.features)
  			(point.label, prediction)
		}
	println("*********** Results of the prediction on the TEST data set **********")
	println("true vs. predicted: ")
	testvaluesAndPreds.foreach(println)
	
	// Evaluation parameters for TEST data set:
	// See detailed descriptions above for training data
	val nTest = testvaluesAndPreds.count()
    // SSE, sum of square errors
    val SSEtest = testvaluesAndPreds.map( vp => math.pow((vp._1 - vp._2), 2) ).
    	reduce(_+_)
    // MSS, model sum of squares
    val modelMeanTest = testvaluesAndPreds.map( vp => math.pow((vp._2), 1) ).
    	reduce(_+_) / (nTest-1)
    val MSStest = testvaluesAndPreds.map( vp => math.pow((modelMean - vp._2), 2) ).
    	reduce(_+_)
    val meanResidualTest = testvaluesAndPreds.map( vp => math.pow((vp._1 - vp._2), 1) ).
    	reduce(_+_) / (nTest-1)
    println("SSE, sum of square errors = " + "%6.3f".format(SSEtest))
   	println("MSS, model sum of squares = " + "%6.3f".format(MSStest))
   	println("r^2, coeff. of det. = " + "%6.3f".format(MSStest / (SSEtest + MSStest)))
   	println("mean residual = " + "%6.3f".format(meanResidualTest))
   	// RMSE, root mean sq error
   	println("Root Mean Squared Error = " + "%6.3f".format(math.sqrt(SSEtest / (nTest-2))))

   	// Save observed and predicted values from test data
   	//testvaluesAndPreds.saveAsTextFile("file:///home/cloudera/Documents/assign11/testOutput");
	
  }
}