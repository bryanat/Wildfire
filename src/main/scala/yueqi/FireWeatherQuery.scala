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

  //data and udf
  var fireDF = ssql.read.parquet("dataset-online/train/randomSampleF0.0002.parquet").select("OBJECTID", "STAT_CAUSE_CODE", "FIRE_NAME", "FIRE_YEAR", "FIRE_SIZE_CLASS", "FIRE_SIZE", "DISCOVERY_DOY", "CONT_DOY")
  var weather = ssql.read.csv("dataset-online/train/randomSampleW0.0002.csv")
  var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk")
  val durationudf = udf((start: Int, end: Int)=>end-start) 
  val percentageudf = udf((part: Int, total: Int)=>part.toFloat/total)
  val monthudf = udf((date:String)=> s"${date(5)}${date(6)}".toInt)

  val weatherDF1 = weatherDF.withColumn("OBJECTID", col("OBJECTID")).withColumn("precip",col("precip").cast(DoubleType)).withColumn("feelslike",col("feelslike").cast(DoubleType)).withColumnRenamed("feelslike", "heatidx").withColumnRenamed("OBJECTID", "OBJECTIDW")
  .withColumn("month", monthudf($"datetime"))
  val combinedDF = weatherDF1.join(fireDF, weatherDF1("OBJECTIDW")===fireDF("OBJECTID"))

//monthly precipitation, heat, and fire analysis
  //how much rain in each month, percentage of days rained in each days
  val totalDaysDF = broadcast(weatherDF1.filter("precip is not NULL").groupBy("month").count().withColumnRenamed("month", "monthT").withColumnRenamed("count", "totaldaycounts"))                                                                                                                                                                                                                                                                                                                                              
  val precipDaysDF = broadcast(weatherDF1.filter("precip is not NULL").filter(weatherDF1("precip")>0).groupBy("month").count().withColumnRenamed("month", "monthP").withColumnRenamed("count", "precipdaycounts"))
  val precipAmountDF = broadcast(weatherDF1.filter("precip is not NULL").groupBy("month").sum("precip").withColumnRenamed("month", "monthA"))
  val heatDF = broadcast(weatherDF1.filter("heatidx is not NULL").groupBy("month").avg("heatidx").withColumnRenamed("month", "monthH"))
  val precipDF = totalDaysDF.join(precipDaysDF, totalDaysDF("monthT")===precipDaysDF("monthP")).drop("monthT")
  .join(precipAmountDF, precipAmountDF("monthA")===precipDaysDF("monthP")).drop("monthP")
  .join(heatDF, precipAmountDF("monthA")===heatDF("monthH")).drop("monthA").withColumnRenamed("monthH", "month")
  //------------------------------------------------------------------------------------------------
  //val precipSumDF = precipDF.select($"month", $"sum(precip)").orderBy(col("month").asc).show()
   //val precipPercDF = precipDF.select($"month", percentageudf($"precipdaycounts", $"totaldaycounts")).withColumnRenamed("UDF(precipdaycounts, totaldaycounts)", "precip_duration").orderBy(col("month").asc).show()
   //val heatAvgDF = precipDF.select($"month", $"avg(heatidx)").orderBy(col("month").asc).show()
  // val naturalCauseDF = combinedDF.filter($"STAT_CAUSE_CODE"==="1.0").groupBy("month").count().orderBy(col("month").asc).show()
  val bigFireDF = combinedDF.filter($"FIRE_SIZE_CLASS"==="F" && $"FIRE_SIZE_CLASS" === "G").groupBy("month").count().orderBy(col("month").asc).show()
  //val durationDF = combinedDF.select("month", "DISCOVERY_DOY", "CONT_DOY").filter("DISCOVERY_DOY is not NULL").filter("CONT_DOY is not NULL").withColumn("fire_duration", durationudf($"DISCOVERY_DOY", $"CONT_DOY")).groupBy("month").avg("duration").orderBy(col("month").asc).show()




  // val yearDF1 = fireDF.groupBy("FIRE_YEAR").count().orderBy(col("count").desc).show(100)
  // val yearDF2 = fireDF.groupBy("FIRE_YEAR", "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_YEAR").desc, col("FIRE_SIZE_CLASS").desc, col("count").desc).show(100)



 

}

}
