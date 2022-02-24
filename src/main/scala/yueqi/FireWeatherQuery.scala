package yueqi
import contexts.ConnectSparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.countDistinct


object FireWeatherQuery {
    val ssql = ConnectSparkSession.connect()
    ssql.sparkContext.setLogLevel("ERROR")
    import ssql.implicits._

def queryFW() {

  //data and udf
  var fireDF = ssql.read.parquet("dataset-online/train/stratifiedSampleF2.parquet").select("OBJECTID", "STAT_CAUSE_CODE", "FIRE_NAME", "FIRE_YEAR", "FIRE_SIZE_CLASS", "FIRE_SIZE", "DISCOVERY_DOY", "CONT_DOY")
  var weather = ssql.read.csv("dataset-online/train/stratifiedSampleW2.csv")
  var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk")
  val durationudf = udf((startday:Int, endday:Int)=>{
        if (endday-startday<0) {365-startday+endday}
        else{
        endday-startday}})
  val percentageudf = udf((part: Int, total: Int)=>part.toFloat/total)
  val monthudf = udf((date:String)=> s"${date(5)}${date(6)}".toInt)

  val weatherDF1 = weatherDF.withColumn("OBJECTID", col("OBJECTID")).withColumn("precip",col("precip").cast(DoubleType)).withColumn("feelslike",col("feelslike").cast(DoubleType)).withColumnRenamed("feelslike", "heatidx").withColumnRenamed("OBJECTID", "OBJECTIDW")
  .withColumn("month", monthudf($"datetime"))
  val combinedDF = weatherDF1.join(fireDF, weatherDF1("OBJECTIDW")===fireDF("OBJECTID"))

//monthly precipitation, heat, and fire analysis
  //total number of fire in each month
  //val totalDaysDF = broadcast(weatherDF1.filter("precip is not NULL").groupBy("month").agg(countDistinct("OBJECTIDW")).withColumnRenamed("month", "monthT").withColumnRenamed("count", "totaldaycounts")).show()                                                                                                                                                                                                                                                                                                                                          
 //
  // val precipDaysDF = broadcast(weatherDF1.filter("precip is not NULL").filter(weatherDF1("precip")>0).groupBy("month").agg(countDistinct("OBJECTIDW")).withColumnRenamed("month", "monthP").withColumnRenamed("count", "precipdaycounts"))

  val precipSmallDF = broadcast(combinedDF.filter("precip is not NULL")
  .filter($"FIRE_SIZE_CLASS"==="A" || $"FIRE_SIZE_CLASS"==="B" || $"FIRE_SIZE_CLASS"==="C" || $"FIRE_SIZE_CLASS"==="D" || $"FIRE_SIZE_CLASS"==="E")
  .groupBy("month").agg(countDistinct("OBJECTIDW").alias("total small fires"), sum("precip").alias("total precipitation"), avg("heatidx").alias("average heat index")).orderBy(col("month").asc)).show()
  val precipLargeDF = broadcast(combinedDF.filter("precip is not NULL")
  .filter($"FIRE_SIZE_CLASS"==="F" || $"FIRE_SIZE_CLASS"==="G")
  .groupBy("month").agg(countDistinct("OBJECTIDW").alias("total large fires"), sum("precip").alias("total precipitation"), avg("heatidx").alias("average heat index")).orderBy(col("month").asc)).show()
  
  //val precipAmountDF = broadcast(weatherDF1.filter("precip is not NULL").groupBy("month").sum("precip").withColumnRenamed("month", "monthA"))
  // val heatDF = broadcast(weatherDF1.filter("heatidx is not NULL").groupBy("month").avg("heatidx").withColumnRenamed("month", "monthH"))
  // val precipDF = totalDaysDF.join(precipDaysDF, totalDaysDF("monthT")===precipDaysDF("monthP")).drop("monthT")
  // .join(precipAmountDF, precipAmountDF("monthA")===precipDaysDF("monthP")).drop("monthP")
  // .join(heatDF, precipAmountDF("monthA")===heatDF("monthH")).drop("monthA").withColumnRenamed("monthH", "month")
  // //------------------------------------------------------------------------------------------------
  //val precipSumDF = precipDF.select($"month", $"sum(precip)").orderBy(col("month").asc).show()
   //val precipPercDF = precipDF.select($"month", percentageudf($"precipdaycounts", $"totaldaycounts")).withColumnRenamed("UDF(precipdaycounts, totaldaycounts)", "precip_duration").orderBy(col("month").asc).show()
   //val heatAvgDF = precipDF.select($"month", $"avg(heatidx)").orderBy(col("month").asc).show()
   //val naturalCauseDF = combinedDF.filter($"STAT_CAUSE_CODE"==="1.0").groupBy("month").count().orderBy(col("month").asc).withColumnRenamed("count", "lightening").show()
  //val bigFireDF = combinedDF.filter($"FIRE_SIZE_CLASS"==="F" || $"FIRE_SIZE_CLASS" === "G").groupBy("month").count().orderBy(col("month").asc).withColumnRenamed("count", "counts of >1000 acre fires").show()

  //val durationDF = combinedDF.select("month", "DISCOVERY_DOY", "CONT_DOY").filter("DISCOVERY_DOY is not NULL").filter("CONT_DOY is not NULL").withColumn("fire_duration", durationudf($"DISCOVERY_DOY", $"CONT_DOY")).groupBy("month").avg("fire_duration").orderBy(col("month").asc).show()





  //val yearDF1 = fireDF.groupBy("FIRE_YEAR").count().orderBy(col("FIRE_YEAR").asc).withColumnRenamed("count", "count of fires").show(100)
  // val yearDF2 = fireDF.groupBy("FIRE_YEAR", "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_YEAR").desc, col("FIRE_SIZE_CLASS").desc, col("count").desc).show(100)



 

}

}
