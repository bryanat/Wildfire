package yueqi
import contexts.ConnectSparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.broadcast
import java.time._


object CorrelationMatrixOps {
    val ssql = ConnectSparkSession.connect()
    import ssql.implicits._



    def pearsonCorr(arrayWF:Array[Row], fireFile:String, weatherFile:String): Unit = {
        def defPearsonCorr(arrayWF:Array[Row]): Unit = {
            var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
            val x = arrayWF.foreach({row=>
                var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, row(13).toString.toDouble, row(14).toString.toDouble) 
                corrArray = corrArray :+ Vectors.dense(temp) 
            })
            val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
            val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
            println("Pearson correlation matrix:\n" + coeff1.toString)
            val matrixRows = coeff1.rowIter.toArray.map(_.toArray)
        }
        defPearsonCorr(fireWeatherCorr(fireFile,weatherFile))
    }




        def spearmanCorr(arrayWF:Array[Row], fireFile:String, weatherFile:String): Unit = {
            def defSpearmanCorr(arrayWF:Array[Row]): Unit = {
                //println(arrayWF.mkString)
            var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
            val x = arrayWF.foreach({row=>
                var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, 
                row(13).toString.toDouble, row(14).toString.toDouble) 
                corrArray = corrArray :+ Vectors.dense(temp) 
            })
            val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
            val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
            println("Spearman correlation matrix:\n" + coeff2.toString) 
            }
            defSpearmanCorr(fireWeatherCorr(fireFile,weatherFile))
    }



    def fireWeatherCorr(fireFile: String, weatherFile:String): Array[Row]={
        //map and udf
        val fireNumMap = Map("A"->0, "B"->1, "C"->2, "D"->3, "E"->4, "F"->5, "G"->6)  
        val classudf = udf((fireclass: String)=>fireNumMap.get(fireclass))
        val burndaysudf = udf((startdate:Int, enddate:Int)=>enddate-startdate)
        val percentageudf = udf((part: Int, total: Int)=>(part/total).toFloat)
        //read files and convert to dataframes
        //7 fire features: fire_size_class, fire_size, lattitude, longitude, fire_year, discovery time, burn days
        val fireDF = ssql.read.parquet(fireFile).select($"OBJECTID", classudf($"FIRE_SIZE_CLASS"),$"FIRE_SIZE", $"LATITUDE",
        $"LONGITUDE",$"FIRE_YEAR",$"DISCOVERY_DOY", burndaysudf($"DISCOVERY_DOY", $"CONT_DOY")).filter("CONT_DOY is not NULL").filter("DISCOVERY_DOY is not NULL").filter("FIRE_SIZE is not NULL").filter("FIRE_YEAR is not NULL")
        var weather = ssql.read.csv(weatherFile)
        var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk")
        //select features and type cast
        val weatherDF1 = weatherDF.withColumn("OBJECTID", col("OBJECTID")).withColumn("tempmax", col("tempmax").cast(DoubleType)).withColumn("tempmin", col("tempmin").cast(DoubleType)).withColumn("dew", col("dew").cast(DoubleType)).withColumn("humidity", col("humidity").cast(DoubleType)).withColumn("precip", 
        col("precip").cast(DoubleType)).withColumn("windspeed", col("windspeed").cast(DoubleType)).withColumn("sealevelpressure", col("sealevelpressure").cast(DoubleType)).withColumn("cloudcover", col("cloudcover").cast(DoubleType)).withColumn("solarradiation", col("solarradiation").cast(DoubleType)).withColumn("solarenergy", col("solarenergy").cast(DoubleType)).withColumn("uvindex", col("uvindex").cast(DoubleType)).
        filter("dew is not NULL").filter("precip is not NULL").filter("cloudcover is not NULL").filter("humidity is not NULL").filter("sealevelpressure is not NULL").filter("windspeed is not NULL").filter("solarradiation is not NULL").filter("solarenergy is not NULL").filter("uvindex is not NULL")     //.withColumn("datetime", col("datetime").cast("timestamp"))
        val avgheat = weatherDF1.groupBy("OBJECTID").avg("feelslike").withColumnRenamed("OBJECTID", "OBJECTID0")
        val avgtempmax = weatherDF1.groupBy("OBJECTID").avg("tempmax").withColumnRenamed("OBJECTID", "OBJECTID1")
        val avgtempmin = weatherDF1.groupBy("OBJECTID").avg("tempmin").withColumnRenamed("OBJECTID", "OBJECTID2")
        val avgdew = weatherDF1.groupBy("OBJECTID").avg("dew").withColumnRenamed("OBJECTID", "OBJECTID3")
        val avghumid = weatherDF1.groupBy("OBJECTID").avg("humidity").withColumnRenamed("OBJECTID", "OBJECTID4")
        val precipAmount = weatherDF1.groupBy("OBJECTID").sum("precip").withColumnRenamed("OBJECTID", "OBJECTID5")
        val avgwindsp = weatherDF1.groupBy("OBJECTID").avg("windspeed").withColumnRenamed("OBJECTID", "OBJECTID6")
        val avgpress = weatherDF1.groupBy("OBJECTID").avg("sealevelpressure").withColumnRenamed("OBJECTID", "OBJECTID7")
        val avgcloud = weatherDF1.groupBy("OBJECTID").avg("cloudcover").withColumnRenamed("OBJECTID", "OBJECTID8")
        val avgradiation = weatherDF1.groupBy("OBJECTID").avg("solarradiation").withColumnRenamed("OBJECTID", "OBJECTID9")
        val avgenergy = weatherDF1.groupBy("OBJECTID").avg("solarenergy").withColumnRenamed("OBJECTID", "OBJECTID10")
        val avguv = weatherDF1.groupBy("OBJECTID").avg("uvindex").withColumnRenamed("OBJECTID", "OBJECTID11")
        val totalDays = broadcast(weatherDF1.groupBy("OBJECTID").count().withColumnRenamed("OBJECTID", "OBJECTID12").withColumnRenamed("count", "totaldaycounts"))                                                                                                                                                                                                                                                       
        val precipDay = broadcast(weatherDF1.filter(weatherDF1("precip")>0).groupBy("OBJECTID").count().withColumnRenamed("OBJECTID", "OBJECTID13").withColumnRenamed("count", "precipdaycounts"))
        val precipPerc = precipDay.join(totalDays, $"OBJECTID12"===$"OBJECTID13").withColumn("precipperc", percentageudf($"precipdaycounts",$"totaldaycounts")).drop("OBJECTID13").drop("totaldaycounts").drop("precipdaycounts")
        //14 weather features: heat, tempmax, tempmin, dew, humid, precip, windsp, pressure, cloud, radiation, energy, uv, precipPercentage
        val weatherJoin = avgheat
            .join(broadcast(avgtempmax), avgtempmax("OBJECTID1")===avgheat("OBJECTID0")).drop("OBJECTID0")
            .join(broadcast(avgtempmin), avgtempmax("OBJECTID1")===avgtempmin("OBJECTID2")).drop("OBJECTID1")
            .join(broadcast(avgdew), avgtempmin("OBJECTID2")===avgdew("OBJECTID3")).drop("OBJECTID2")
            .join(broadcast(avghumid), avgdew("OBJECTID3")===avghumid("OBJECTID4")).drop("OBJECTID3")
            .join(broadcast(precipAmount), avghumid("OBJECTID4")===precipAmount("OBJECTID5")).drop("OBJECTID4")
            .join(broadcast(avgwindsp), precipAmount("OBJECTID5")===avgwindsp("OBJECTID6")).drop("OBJECTID5")
            .join(broadcast(avgpress), avgwindsp("OBJECTID6")===avgpress("OBJECTID7")).drop("OBJECTID6")
            .join(broadcast(avgcloud), avgpress("OBJECTID7")===avgcloud("OBJECTID8")).drop("OBJECT7")
            .join(broadcast(avgradiation), avgcloud("OBJECTID8")===avgradiation("OBJECTID9")).drop("OBJECT8")
            .join(broadcast(avgenergy), avgradiation("OBJECTID9")===avgenergy("OBJECTID10")).drop("OBJECT9")
            .join(broadcast(avguv), avgenergy("OBJECTID10")===avguv("OBJECTID11")).drop("OBJECT10")
            .join(broadcast(precipPerc), avguv("OBJECT11")===precipPerc("OBJECT12")).drop("OBJECT11")

        //drop objectid on column ~12
        val joinFW = fireDF.join(weatherJoin, fireDF("OBJECTID")===weatherJoin("OBJECTID12")).drop("OBJECTID12").drop("OBJECTID").collect()
        println(joinFW.mkString)
        joinFW
    }   


    def fireOnlyCorr(): Unit= {
        val fireDF = ssql.read.parquet("dataset-online/train/stratifiedSampleAll2.parquet").select("FIRE_SIZE", "LATITUDE",
        "LONGITUDE","FIRE_YEAR","DISCOVERY_DOY","CONT_DOY").filter("CONT_DOY is not NULL").filter("DISCOVERY_DOY is not NULL").filter("FIRE_SIZE is not NULL")
        var corrArray = Seq(Vectors.dense(0,0,0))
        var corr = fireDF.collect().foreach({row=>
            var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble) 
            corrArray = corrArray :+ Vectors.dense(temp)  
        })
        val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
        val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
        println("Pearson correlation matrix:\n" + coeff1.toString)
        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
        println("Spearman correlation matrix:\n" + coeff2.toString)
    }




    }




