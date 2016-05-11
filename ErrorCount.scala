package sparklambda.common

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object ErrorCount {
  def countErrors(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .filter(_.contains("ERROR")) // Keep "ERROR" lines
      .map( s => (s.split(" ")(0), 1) ) // Return tuple with date & count
      .reduceByKey(_+_) // Sum counts for each date
  }
}
//Done01

