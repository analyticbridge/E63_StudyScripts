package sparklambda.etl
import sparklambda.ErrorCount
import org.apache.spark.SparkConf


object BatchErrorCount {
  def main(args: Array[String]): Unit = {
    if (args.length<3) {
      System.err.println("Usage: BatchErrorCount <master> <inputfile> <outputfile>")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)
      .setJars(Seq(SparkContext.jarOfClass(this.getClass).get))

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(1))

    val errCount = ErrorCount.countErrors(lines)

    errCount.saveAsTextFile(args(2))
  }
}
//Done01