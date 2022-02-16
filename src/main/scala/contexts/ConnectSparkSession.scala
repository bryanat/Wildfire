package contexts

import org.apache.spark.sql._

// Want to rename these but for sake of time right now the names remain
object ConnectSparkSession {
  def connect(): SparkSession = {
    // val sconf = new SparkConf().setMaster("local[4]").setAppName("Wildfire").setSparkHome("C:\\Spark")
    // sconf.set("spark.driver.memory", "4g") 
    
    // val sc = new SparkContext(sconf)
    
    // val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()
    val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").enableHiveSupport().getOrCreate()
    ssql
  }
}
