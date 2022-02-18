package yueqi
import org.apache.spark._
import org.apache.spark.sql._

object fireWeather {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val ssql = SparkSession.builder()
      .appName("WildFire")
      .config("spark.master", "local") 
      .enableHiveSupport()
      .getOrCreate()
  var fireDF = ssql.read.parquet("dataset-online/train/fireG.parquet")
  ////waiting on weatherG.csv to be generated
  //var weatherDF = ssql.read.csv("dataset-online/train/weatherG.csv")
}
