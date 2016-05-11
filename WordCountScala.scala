/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package edu.hu.examples

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCountScala {

	// Function to change string to lower case
	def fnlcase(x:String)=x.toLowerCase

	// Function to strip off all punctuation characters	
	// Only keep letter and digit characters and space 
	def fnpattern(x:String)= 
	{
	  val strippattern="[^a-zA-Z0-9 ]".r 
	  strippattern.replaceAllIn(x, "")
	}

    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCountScala")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val input =  sc.textFile(inputFile)

  	// Clean up the input lines 
	// Remove all punctuation characters and change all to lower case
	val cleanInput = input.map(line => fnlcase(fnpattern(line)))

      // Split up into words.
      val words = cleanInput.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile(outputFile)
    }
}
