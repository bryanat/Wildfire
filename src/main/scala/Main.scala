import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import dataops.GetWeather
import yueqi.Chisquare
import yueqi.FireWeather
import abby.Whatever
import org.apache.spark.sql.DataFrameWriter
// import org.apache.spark.sql.SparkSession.implicits._
import dataops.DataOps
import abby.AbbysDataops
import dataops.Sampling


object Main {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
    // val sconf = new SparkConf().setMaster("local[4]").setAppName("Wildfire").setSparkHome("C:\\Spark")
    // sconf.set("spark.driver.memory", "4g") 
    
    // val sc = new SparkContext(sconf)
    
    // val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()
    //val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").enableHiveSupport().getOrCreate()
    
    //Whatever.testprint()
    //Yueqi.testprint()

    //val sconf = new SparkConf().setMaster("local[4]").setAppName("P2").setSparkHome("C:\\Spark")
    //val sc = new SparkContext(sconf)

    //GetWeather.getWeather("dataset/train/fireG10.parquet", "dataset/testweather.csv")
    //Sampling.stratifiedSampling("dataset-online/train/fireG.parquet", "dataset-offline/train/sample4")
    //Sampling.randomSampling("dataset-online/train/fireG.parquet", "dataset-offline/train/sample5")

    //Yueqi's functions
    //Chisquare.fireSizeAndCause()
    //FireWeather.combineFireWeather()
    //Chisquare.fireSizeAndWeather()



    //Abby's functions
   // AbbysDataops.runSummaryStatements()


  }
}
