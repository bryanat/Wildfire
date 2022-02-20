package yueqi
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FireWeather {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val ssql = SparkSession.builder()
      .appName("WildFire")
      .config("spark.master", "local") 
      .enableHiveSupport()
      .getOrCreate()

def combineFireWeather() {
  var fireDF = ssql.read.parquet("dataset-offline/train/fireG.parquet")
  var weather = ssql.read.csv("dataset-offline/train/testWeather3.csv")
  var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk","sunrise","sunset","moonphase","conditions" )
  var bcWeather = ssql.sparkContext.broadcast(weatherDF)
  var joinFW = fireDF.join(weatherDF, weatherDF("OBJECTID")===fireDF("OBJECTID")).select("FIRE_NAME", "FIRE_YEAR", "FIRE_SIZE_CLASS", "datetime", "tempmax", "precip")
    joinFW.show(100)
}

}
