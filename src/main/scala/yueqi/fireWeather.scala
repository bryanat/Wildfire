package yueqi
import org.apache.spark._
import org.apache.spark.sql._

object fireWeather {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark1 = SparkSession.builder()
      .appName("WildFire")
      .config("spark.master", "local") 
      .enableHiveSupport()
      .getOrCreate()
  var fireDF = spark1.read.parquet("dataset/train/fireG.parquet")
  var weatherDF = spark1.read.csv("dataset/train/weatherG.csv")
}
