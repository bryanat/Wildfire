import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import abby.Whatever
import yueqi.Yueqi
import org.apache.spark.sql.DataFrameWriter
// import org.apache.spark.sql.SparkSession.implicits._
import dataops.DataOps


object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    // val sconf = new SparkConf().setMaster("local[4]").setAppName("Wildfire").setSparkHome("C:\\Spark")
    // sconf.set("spark.driver.memory", "4g") 
    
    // val sc = new SparkContext(sconf)
    
    // val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()
    val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").enableHiveSupport().getOrCreate()
    
    //Whatever.testprint()
    //Yueqi.testprint()

    // Test comment for a test commit for brandon to test pull request and merge master into brandon


  }
}
