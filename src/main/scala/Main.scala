import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import yueqi.Chisquare
import yueqi.TestCorrelation
import yueqi.FireWeather
import abby.Whatever
import org.apache.spark.sql.DataFrameWriter
// import org.apache.spark.sql.SparkSession.implicits._
import dataops.{GetWeather, DataOps, Sampling, QueryFire}
import abby.AbbysDataops


object Main {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
    
    // val sconf = new SparkConf().setMaster("local[4]").setAppName("Wildfire").setSparkHome("C:\\Spark")
    // sconf.set("spark.driver.memory", "4g") 
    // val sc = new SparkContext(sconf)
    // val sconf = new SparkConf().setMaster("local[4]").setAppName("P2").setSparkHome("C:\\Spark")
    // val sc = new SparkContext(sconf)
    // val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()

    // val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").enableHiveSupport().getOrCreate()

    //Master Main Space
    //GetWeather.getWeather("dataset-offline/train/randomSample0.0002.parquet", "dataset-offline/train/randomSampleweather2.csv")
    
    //Yueqi's Main Space
    //Chisquare.fireSizeAndCause()
    //DataOps.createJSONFile()
    //Sampling.stratifiedSampling("dataset-online/train/WildfireAll.parquet", "dataset-offline/train/sample")
    //Sampling.randomSampling("dataset-online/train/WildfireAll.parquet", "dataset-offline/train/sample2")
    //FireWeather.combineFireWeather()
    TestCorrelation.fireWeatherCorr()


    
    //Abby's Main Space
    //AbbysDataops.runSummaryStatements()

    //Brandon's Main Space

    QueryFire.queryTexas()
  }
}
