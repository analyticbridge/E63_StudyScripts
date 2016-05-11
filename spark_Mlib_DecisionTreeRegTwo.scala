package edu.hu.examples

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics //Notice
import org.apache.spark.mllib.linalg.distributed.RowMatrix //BetterLR
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.tree.model.DecisionTreeModel

object DecisionTreeRegTwo {
  def main(args: Array[String]) {

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("linearreg")
    val sc = new SparkContext(conf)

    // Read the input file from local dir, its a csv file without header
	val rawtrainrdd = sc.textFile("file:///home/cloudera/Documents/assign11/data2.csv")
	
	// Split data randomly into training (90%) and test (10%).
	val splits = rawtrainrdd.randomSplit(Array(0.9, 0.1), seed = 1) //what is 11L?
	val training = splits(0).cache()
	val test = splits(1).cache()
	val testsize = test.count()
	val trainingsize = training.count()
	
	// Print out the test size and training size to show the # of data in each
	println("test size: " + testsize)
	println("training size: " + trainingsize)

	// Target 2: acceleration, 1
	// parts(4) is horsepower, which is the target variable
	
	// Features:
	// displacement, quantitative, 3
	// cylinders, quantitative, 2
	// model_year, quantitative, 7
	// weight, quantitative, 10
	// origin, categorical, 9
	// manufacturer, categorical, 5

	// split data at commas	
	val parts = training.map(x => x.split(","))
	val alldata = rawtrainrdd.map(x => x.split(","))
	
	// Find all the distinct levels in the categorical variable
	val originCategories = alldata.map(r => r(9)).distinct.collect.zipWithIndex.toMap
	val manufacturerCategories = alldata.map(r => r(5)).distinct.collect.zipWithIndex.toMap
	
	// Get the total number of levels
    val numCategoriesOrigin = originCategories.size
    println("the number of levels for Origin: " + numCategoriesOrigin)
    val numCategoriesManufacturer = manufacturerCategories.size
    println("the number of levels for Manufacturer: " + numCategoriesManufacturer)

	// Create LabeledPoint RDD
    val parsedData = parts.map { r =>
      
      	// Get which index should be changed
      	val categoryIdxOrigin = originCategories(r(9))
      	val categoryIdxManufacturer = manufacturerCategories(r(5))
      	
      	// Make an array of zeros with a size of the total number of levels
      	val categoryFeaturesOrigin = Array.ofDim[Double](numCategoriesOrigin)
      	val categoryFeaturesManufacturer = Array.ofDim[Double](numCategoriesManufacturer)
      	
      	// For the given index, turn this on (i.e. make it 1)
      	categoryFeaturesOrigin(categoryIdxOrigin) = 1.0
      	categoryFeaturesManufacturer(categoryIdxManufacturer) = 1.0
      	
		// map data for quantitative variables/features
		val disp = r(3).split(' ').map(_.toDouble)
		val cyl = r(2).split(' ').map(_.toDouble)
		val modelyear = r(7).split(' ').map(_.toDouble)
		val weight = r(10).split(' ').map(_.toDouble)
	
		// combine features
		val features = disp ++ cyl ++ modelyear ++ weight ++ categoryFeaturesOrigin ++ categoryFeaturesManufacturer
		
      	// Target 1: horsepower, 4
      	LabeledPoint(r(4).toInt, Vectors.dense(features))
      	
      	// Target 2: acceleration, 1
      	//LabeledPoint(r(1).toDouble, Vectors.dense(features))
      	
    }
	
	parsedData.cache()
	
	println("Labeled Point Example from Training Set: ")
	parsedData.take(1).foreach(println)
	
	// Create LabeledPoint RDD for the test data set
	val testparts = test.map(x => x.split(","))
	
	val parsedTestData = testparts.map { r =>
      
      	// Get which index should be changed
      	val categoryIdxOrigin = originCategories(r(9))
      	val categoryIdxManufacturer = manufacturerCategories(r(5))
      	
      	// Make an array of zeros with a size of the total number of levels
      	val categoryFeaturesOrigin = Array.ofDim[Double](numCategoriesOrigin)
      	val categoryFeaturesManufacturer = Array.ofDim[Double](numCategoriesManufacturer)
      	
      	// For the given index, turn this on (i.e. make it 1)
      	categoryFeaturesOrigin(categoryIdxOrigin) = 1.0
      	categoryFeaturesManufacturer(categoryIdxManufacturer) = 1.0
      	
		// map data for quantitative variables/features
		val disp = r(3).split(' ').map(_.toDouble)
		val cyl = r(2).split(' ').map(_.toDouble)
		val modelyear = r(7).split(' ').map(_.toDouble)
		val weight = r(10).split(' ').map(_.toDouble)
	
		// combine features
		val features = disp ++ cyl ++ modelyear ++ weight ++ categoryFeaturesOrigin ++ categoryFeaturesManufacturer
		
		// Target 1: horsepower, 4
      	LabeledPoint(r(4).toInt, Vectors.dense(features))
      	
      	// Target 2: acceleration, 1
      	//LabeledPoint(r(1).toDouble, Vectors.dense(features))
    }
	
	parsedTestData.cache()
	
	println("Labeled Point Example from Test Set: ")
	parsedTestData.take(1).foreach(println)
	
    // Train a DecisionTree model.
	// Empty categoricalFeaturesInfo indicates all features are continuous.
	val categoricalFeaturesInfo = Map[Int, Int]()
	val impurity = "variance"
	val maxDepth = 5 //*****
	val maxBins = 32 //*****

	val model = DecisionTree.trainRegressor(parsedData, categoricalFeaturesInfo, impurity,
  		maxDepth, maxBins)
  	
  	println("Learned regression tree model:\n" + model.toDebugString)

	// Save and load model
	//model.save(sc, "target/tmp/myDecisionTreeRegressionModel")
	//val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeRegressionModel")
	
    // Predict using the test data and compute the test error
   	val testvaluesAndPreds = parsedTestData.map { point =>
  		val prediction = model.predict(point.features)
  			(point.label, prediction)
		}
	println("*********** Results of the prediction on the TEST data set **********")
	println("true vs. predicted: ")
	testvaluesAndPreds.foreach(println)
	
	// Evaluation parameters for TEST data set:
	val nTest = testvaluesAndPreds.count()
    // SSE, sum of square errors
    val SSEtest = testvaluesAndPreds.map( vp => math.pow((vp._1 - vp._2), 2) ).
    	reduce(_+_)
    // MSS, model sum of squares
    val modelMeanTest = testvaluesAndPreds.map( vp => math.pow((vp._2), 1) ).
    	reduce(_+_) / (nTest-1)
    val MSStest = testvaluesAndPreds.map( vp => math.pow((modelMeanTest - vp._2), 2) ).
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
	
	//parsedData.saveAsTextFile("file:///home/cloudera/Documents/assign11/output01")

	
  }
}