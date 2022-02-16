import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import yueqi.Yueqi


object Main {
  def main(args: Array[String]): Unit = {
    println("Start of P2")

  //  val sconf = new SparkConf().setMaster("local[4]").setAppName("P2").setSparkHome("C:\\Spark")
//    val sc = new SparkContext(sconf)

    val ssql = SparkSession.builder().appName("HiveApp").config("spark.master", "local").enableHiveSupport().getOrCreate()
      

    var df = ssql.sql("SELECT * FROM Wildfire")
    df.show()
    Yueqi.testprint()


  }
}