package sparklambda.streaming

import sparklambda.ErrorCount
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD


object StreamingErrorCount extends Logging{

  def main(args: Array[String]): Unit = {


   if (args.length < 3) {
      System.err.println("Usage: StreamingErrorCount <master> <hostname> <port>")
      System.exit(1)
    }

    //Configure the Streaming Context

    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)

    setStreamingLogLevels()


    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")

    // Create the DStream from data sent over the network
    val dStream = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    // Counting the errors in each RDD in the stream
    val errCountStream = dStream.transform(rdd => ErrorCount.countErrors(rdd))


    // printing out the current error count
    errCountStream.foreachRDD(rdd => {
      System.out.println("Errors this minute:%d".format(rdd.first()._2))
    })

    // creating a stream with running error count
    val stateStream = errCountStream.updateStateByKey[Int](updateFunc)

    // printing the running error count
    stateStream.foreachRDD(rdd => {
      System.out.println("Errors today:%d".format(rdd.first()._2))
    })

    // starting the action
    ssc.start()
    ssc.awaitTermination()
  }


  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)

    val previousCount = state.getOrElse(0)

    Some(currentCount + previousCount)
  }

}
//Done01