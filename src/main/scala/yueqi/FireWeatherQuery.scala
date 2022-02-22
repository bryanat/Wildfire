package yueqi
import contexts.ConnectSparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._

object FireWeatherQuery {
    val ssql = ConnectSparkSession.connect()
    import ssql.implicits._

def queryFW() {
  var fireDF = ssql.read.parquet("dataset-online/train/randomSampleF0.0002.parquet")
  var weather = ssql.read.csv("dataset-online/train/randomSampleW0.0002.csv")
  var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk")//.partitionBy(new HashPartitioner(100)).persist(MEMORY_AND_DISK_SER)
  val weatherDF1 = weatherDF.select("OBJECTID","tempmax", "tempmin", "dew", "humidity", "precip", "windspeed", "sealevelpressure", "cloudcover", "solarradiation", "solarenergy", "uvindex").withColumn("tempmax", col("tempmax").cast(DoubleType)).withColumn("tempmin", col("tempmin").cast(DoubleType)).withColumn("dew", col("dew").cast(DoubleType)).withColumn("humidity", col("humidity").cast(DoubleType)).withColumn("precip", 
        col("precip").cast(DoubleType)).withColumn("windspeed", col("windspeed").cast(DoubleType)).withColumn("sealevelpressure", col("sealevelpressure").cast(DoubleType)).withColumn("cloudcover", col("cloudcover").cast(DoubleType)).withColumn("solarradiation", col("solarradiation").cast(DoubleType)).withColumn("solarenergy", col("solarenergy").cast(DoubleType)).withColumn("uvindex", col("uvindex").cast(DoubleType))  
  var joinFW = weatherDF1.join(fireDF, weatherDF("OBJECTID")===fireDF("OBJECTID")).select("FIRE_NAME", "FIRE_YEAR", "FIRE_SIZE_CLASS", "FIRE_SIZE", "STATE", "DISCOVERY_DOY","CONT_DOY",
  "datetime", "tempmax", "tempmin", "dew", "humidity", "precip", "windspeed", "sealevelpressure", "solarradiation", "solarenergy", "uvindex")


  //duration trend: how long each fire lasted vs fire class and fire size
  val durationudf = udf((start: Int, end: Int)=>end-start)
  val duration = fireDF.filter("DISCOVERY_DOY is not NULL").filter("CONT_DOY is not NULL").withColumn("DURATION", durationudf($"DISCOVERY_DOY", $"CONT_DOY")).orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_SIZE").desc)
  val durationDF = duration.dropDuplicates("FIRE_NAME").select("FIRE_NAME", "FIRE_YEAR", "DURATION", "FIRE_SIZE_CLASS", "FIRE_SIZE").show()
  //year trends: order by years, count numbers of classes
  val yearDF = fireDF.groupBy("FIRE_YEAR", "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_YEAR").desc,col("FIRE_SIZE_CLASS").desc).show()
  //distribution trend: group by state, count number of classes
  val stateDF = fireDF.groupBy("STATE", "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_YEAR").desc,col("FIRE_SIZE_CLASS").desc).show()
  //weather trends vs fire trends: heat index
  //weather during class G fire: 
  val tempDF = joinFW.groupBy("OBJECTID").avg("temp").withColumnRenamed("OBJECTID", "OBJECTID1")
  val heatDF = joinFW.groupBy("OBJECTID").avg("humidity").withColumnRenamed("OBJECTID", "OBJECTID2")
  val feelsDF = joinFW.groupBy("OBJECTID").avg("feelslike").withColumnRenamed("OBJECTID", "OBJECTID3").withColumnRenamed("feelslike", "heatindex")
  val humidTempDF = tempDF.join(broadcast(heatDF), tempDF("OBJECTID1")===heatDF("OBJECTID2")).drop("OBJECTID1").join(broadcast(feelsDF), feelsDF("OBJECTID3")===heatDF("OBJECTID2")).drop("OBJECTID2").withColumnRenamed("OBJECTID3", "OBJECTID").show()
  //val heatidxudf = udf((T:Double, RH: Double)=>-42.379 + 2.04901523*T + 10.14333127*RH - .22475541*T*RH - .00683783*T*T - .05481717*RH*RH + .00122874*T*T*RH + .00085282*T*RH*RH - .00000199*T*T*RH*RH)
  



}

}
