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


object CorrelationMatrixOps {
    val ssql = ConnectSparkSession.connect()
    import ssql.implicits._

    def fireOnlyCorr(): Unit = {
        val fireDF = ssql.read.parquet("dataset-offline/train/stratifiedSampleAll2.parquet").select("FIRE_SIZE", "LATITUDE",
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


//Fire_Size_Class 1. Fire_Size 2. Lattitude 3. Longitude 4. Fire_year 5. Discovery_doy 6. Cont_doy 7. avgtempmax 8. avgtempmin 9. avgdew 10. avghumid 11. avgprecip 12. avgwindsp 13. avgpress 14. avgcloud
    def fireWeatherCorr(): Unit={
        val fireNumMap = Map("A"->0, "B"->1, "C"->2, "D"->3, "E"->4, "F"->5, "G"->6)  
        val classudf = udf((fireclass: String)=>fireNumMap.get(fireclass))
        val fireDF = ssql.read.parquet("dataset-offline/train/randomSampleF0.0002.parquet").select($"OBJECTID", classudf($"FIRE_SIZE_CLASS"),$"FIRE_SIZE", $"LATITUDE",
        $"LONGITUDE",$"FIRE_YEAR",$"DISCOVERY_DOY",$"CONT_DOY").filter("CONT_DOY is not NULL").filter("DISCOVERY_DOY is not NULL").filter("FIRE_SIZE is not NULL").filter("FIRE_YEAR is not NULL")
        var weatherFile = ssql.read.csv("dataset-offline/train/randomSampleW0.0002.csv")
        var weatherDF = weatherFile.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk")
        val weatherDF1 = weatherDF.select("OBJECTID","tempmax", "tempmin", "dew", "humidity", "precip", "windspeed", "sealevelpressure", "cloudcover").withColumn("tempmax", col("tempmax").cast(DoubleType)).withColumn("tempmin", col("tempmin").cast(DoubleType)).withColumn("dew", col("dew").cast(DoubleType)).withColumn("humidity", col("humidity").cast(DoubleType)).withColumn("precip", 
        col("precip").cast(DoubleType)).withColumn("windspeed", col("windspeed").cast(DoubleType)).withColumn("sealevelpressure", col("sealevelpressure").cast(DoubleType)).withColumn("cloudcover", col("cloudcover").cast(DoubleType)).filter("dew is not NULL").filter("precip is not NULL").filter("cloudcover is not NULL").filter("humidity is not NULL").filter("sealevelpressure is not NULL").filter(
            "windspeed is not NULL")
        val avgtempmax = weatherDF1.groupBy("OBJECTID").avg("tempmax").withColumnRenamed("OBJECTID", "OBJECTID1")
        val avgtempmin = weatherDF1.groupBy("OBJECTID").avg("tempmin").withColumnRenamed("OBJECTID", "OBJECTID2")
        val avgdew = weatherDF1.groupBy("OBJECTID").avg("dew").withColumnRenamed("OBJECTID", "OBJECTID3")
        val avghumid = weatherDF1.groupBy("OBJECTID").avg("humidity").withColumnRenamed("OBJECTID", "OBJECTID4")
        val avgprecip = weatherDF1.groupBy("OBJECTID").avg("precip").withColumnRenamed("OBJECTID", "OBJECTID5")
        val avgwindsp = weatherDF1.groupBy("OBJECTID").avg("windspeed").withColumnRenamed("OBJECTID", "OBJECTID6")
        val avgpress = weatherDF1.groupBy("OBJECTID").avg("sealevelpressure").withColumnRenamed("OBJECTID", "OBJECTID7")
        val avgcloud = weatherDF1.groupBy("OBJECTID").avg("cloudcover").withColumnRenamed("OBJECTID", "OBJECTID8")
        val weatherJoin = avgtempmax.join(avgtempmin, avgtempmax("OBJECTID1")===avgtempmin("OBJECTID2"))
            .join(avgdew, avgtempmin("OBJECTID2")===avgdew("OBJECTID3"))
            .join(avghumid, avgdew("OBJECTID3")===avghumid("OBJECTID4"))
            .join(avgprecip, avghumid("OBJECTID4")===avgprecip("OBJECTID5"))
            .join(avgwindsp, avgprecip("OBJECTID5")===avgwindsp("OBJECTID6"))
             .join(avgpress, avgwindsp("OBJECTID6")===avgpress("OBJECTID7"))
             .join(avgcloud, avgpress("OBJECTID7")===avgcloud("OBJECTID8")).drop("OBJECT1").drop("OBJECT2").drop("OBJECT3").drop("OBJECT4").drop("OBJECT5").drop("OBJECT6").drop("OBJECT7").withColumnRenamed("OBJECTID8", "OBJECTID")
        //val arrayWF = fireDF.join(weatherJoin, fireDF("OBJECTID")===weatherJoin("OBJECTID")).collect()
        //PearsonCorr(arrayWF)
        var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0))
        val joinFW = fireDF.join(weatherJoin, fireDF("OBJECTID")===weatherJoin("OBJECTID")).collect().foreach({row=>
            var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
            row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, row(13).toString.toDouble, row(14).toString.toDouble)
            corrArray = corrArray :+ Vectors.dense(temp) 
        })
        val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
        df.show()
        val Row(coeff1: Matrix) = Correlation.corr(df, "features")
        println(s"Pearson correlation matrix:\n $coeff1")
        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman")
        println(s"Spearman correlation matrix:\n $coeff2")
    }   


    // def PearsonCorr(arrayWF:Array[Row]): Unit = {
    //     var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0))
    //     val x = arrayWF.foreach({row=>
    //         var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
    //         row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, row(13).toString.toDouble) 
    //         corrArray = corrArray :+ Vectors.dense(temp) 
    //     })
    //     val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
    //     val Row(coeff1: Matrix) = Correlation.corr(df, "features")
    //     println("Pearson correlation matrix:\n" + coeff1.toString)
    // }

    // def SpearmanCorr(arrayWF:Array[Row]): Unit = {
    //     var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0))
    //     val x = arrayWF.foreach({row=>
    //         var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
    //         row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, row(13).toString.toDouble) 
    //         corrArray = corrArray :+ Vectors.dense(temp) 
    //     })
    //     val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
    //     val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman")
    //     println("Spearman correlation matrix:\n" + coeff2.toString) 

    // }



//     def sizeAndAvgTemp(): Unit = {
//     import ssql.implicits._
//     val fireDF = ssql.read.parquet("dataset-offline/train/randomSample0.0002.parquet")
//     val weather = ssql.read.csv("dataset-offline/train/randomSampleweather2.csv").toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
//   "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex", "severerisk")
//     var weatherDF = weather.withColumn("tempmax", col("tempmax").cast(DoubleType))
//    var avgMaxTemp = weatherDF.groupBy("OBJECTID").avg("tempmax").withColumnRenamed("avg(tempmax)", "avgmaxtemp")
//    avgMaxTemp.show()

//    //avg("tempmin").avg("dew").avg("humidity").avg("windspeed").avg("cloudcover")
//    //.withColumnRenamed("avg(tempmin)", "avgmintemp").withColumnRenamed("avg(humidity)", "avghumid").withColumnRenamed("avg(windspeed)", "avgwindspd").withColumnRenamed("avg(cloudcover)", "avgcloud")
   
//    var fireSizeArray = ArrayBuffer[Double]()
//    var avgTempArray = ArrayBuffer[Double]()
//     var joinFW = fireDF.join(avgMaxTemp, avgMaxTemp("OBJECTID")===fireDF("OBJECTID"), "inner").select("FIRE_SIZE", "avgmaxtemp").collect().foreach({row=>
//         var size = row(0).toString.toDouble
//         var temp = row(1).toString.toDouble
//         fireSizeArray+=size 
//         avgTempArray+=temp
//     })
//     var corrVector = Seq(Vectors.dense(0,0,0))
//     corrVector = corrVector :+ Vectors.dense(fireSizeArray.toArray)
//     corrVector = corrVector :+ Vectors.dense(avgTempArray.toArray)
//     val df = corrVector.drop(1).map(Tuple1.apply).toDF("features")
//     val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
//     println("Pearson correlation matrix:\n" + coeff1.toString)
//     val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
//     println("Spearman correlation matrix:\n" + coeff2.toString)
//     }


    }




