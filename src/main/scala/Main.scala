import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrameWriter
import yueqi.{CorrelationMatrixOps, Chisquare, FireWeatherQuery, LogRegressionOps}
import dataops.{DataOps, GetWeather, Sampling} 
import abby.AbbysDataops
import org.sparkproject.dmg.pmml.CorrelationMethods

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
    //DataOps.createJSONFile()
    GetWeather.getWeather("dataset-offline/train/stratifiedSampleF3.parquet", "dataset-offline/train/stratifiedSampleW3.csv") 
    //GetWeather.getWeather("dataset-offline/train/fireG10.parquet", "dataset-offline/train/fireG10.csv") 
    //Sampling.stratifiedSampling("dataset-online/train/WildfireAll.parquet", "dataset-offline/train/sample")
    //Sampling.randomSampling("dataset-online/train/WildfireAll.parquet", "dataset-offline/train/sample5")

    //Yueqi's Main Space
    //Chisquare.fireStateChi()
    //Chisquare.fireCauseChi()
    //CorrelationMatrixOps.pearsonCorr(CorrelationMatrixOps.fireWeatherCorr("dataset-online/train/stratifiedSampleF2.parquet", "dataset-online/train/stratifiedSampleW2.csv"),"dataset-online/train/stratifiedSampleF2.parquet", "dataset-online/train/stratifiedSampleW2.csv")
    //LogRegressionOps.fitClassAndWeather()
    //FireWeatherQuery.queryFW()
    //Stream.stream()



    
    //Abby's Main Space
    //AbbysDataops.runSummaryStatements()

    //Brandon's Main Space

  }
}
