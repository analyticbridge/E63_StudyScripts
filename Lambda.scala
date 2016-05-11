import java.nio.ByteBuffer
import java.util.Date

import scala.util.Random
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.parsing.json.JSON
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.io.compress._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


private object RealTime extends Logging {
  def main(args: Array[String]) {
    /* Check that all required args were passed in. */
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: RealTime <stream-name> <endpoint-url>
        """.stripMargin)
      System.exit(1)
    }

    val Array(streamName, endpointUrl) = args
    		
    val batchInterval = Seconds(60) 
    val sparkConfig = new SparkConf().setAppName("RealTimeExample").set("spark.driver.allowMultipleContexts","true")
		//.setMaster(s"local[$numSparkThreads]")

	val sql = new SparkContext(sparkConfig)
	val sqlContext = new SQLContext(sql)
	val ssc = new StreamingContext(sparkConfig, batchInterval)	
    ssc.checkpoint("checkpoint") 
	
	

    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
	kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards()
      .size()
   
    val numStreams = numShards
	val numSparkThreads = numStreams + 1

		   
	Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
	Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val kinesisCheckpointInterval = batchInterval

    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, streamName, endpointUrl, kinesisCheckpointInterval,
          InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
    } 

    val unionStreams = ssc.union(kinesisStreams)

    val jsonStream = unionStreams.flatMap(byteArray => new String(byteArray).split("\n"))

    val statuses = jsonStream.map(status => status.toString())


    val outDateFormat =  new java.text.SimpleDateFormat("yyyy/MM/dd/HH/mm")
 	
    val yearFormat = new java.text.SimpleDateFormat("yyyy")
    val monthFormat = new java.text.SimpleDateFormat("MM")
    val dayFormat =new java.text.SimpleDateFormat("dd")
    val hourFormat = new java.text.SimpleDateFormat("HH")
    val minFormat = new java.text.SimpleDateFormat("mm")
	

	jsonStream.foreachRDD((rdd,time) => {
	  if (rdd.count > 0) {
	                
	        val sqlRDD = sqlContext.jsonRDD(rdd)
	        sqlRDD.registerTempTable("records")
			sqlContext.sql("SELECT ProductName,price FROM records WHERE price >= 40 AND price <= 50").collect().foreach(println)
		  	val outPartitionFolder = outDateFormat.format(new Date(time.milliseconds))
					  
			val yearFormatFolder = yearFormat.format(new Date(time.milliseconds))
			val monthFormatFolder = monthFormat.format(new Date(time.milliseconds))
			val dayFormatFolder = dayFormat.format(new Date(time.milliseconds))
			val hourFormatFolder = hourFormat.format(new Date(time.milliseconds))
			val minFormatFolder = minFormat.format(new Date(time.milliseconds))
					  
		  	rdd.coalesce(1).saveAsTextFile("%s/year=%s/month=%s/day=%s/hour=%s/min=%s".format("hdfs:///kinesis-stream", yearFormatFolder, monthFormatFolder,dayFormatFolder,hourFormatFolder,minFormatFolder),
					classOf[GzipCodec])
//Saving to S3
//			rdd.coalesce(1).saveAsTextFile("%s/year=%s/month=%s/day=%s/hour=%s/min=%s".format("s3://chayel-emr/historical/kinesis-stream", yearFormatFolder, monthFormatFolder,dayFormatFolder,hourFormatFolder,minFormatFolder),classOf[GzipCodec])
					
	        } else {
	                println("No message received")
	        }
	}) 
		

				
			
    /* Start the streaming context and await termination */
    ssc.start()
    ssc.awaitTermination()
  }
}


object Historical {
 def main(args: Array[String]) {

	 //Check if all the arguments are passed
	    if (args.length < 1) {
	        System.err.println(
	          """
	            |Usage: Historical <s3 bucket>>
	          """.stripMargin)
	        System.exit(1)
	      }
	    
	    val Array(s3BucketName) = args
	Logger.getLogger("org").setLevel(Level.ERROR)
    		
        val conf = new SparkConf().setAppName("HistoricalExample")
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.createSchemaRDD

        val wData = sqlContext.jsonFile("s3://"+ s3BucketName +"/historical/*/*/*/*/*/*")
        wData.printSchema()
        wData.registerAsTable("all_data")

        val wcount = sqlContext.sql("SELECT count(ProductName) FROM all_data WHERE price >= 40 AND price <= 50")

        wcount.map(t => "Count of all products between 40 and 50: " + t(0)).collect().foreach(println)

  }
}
