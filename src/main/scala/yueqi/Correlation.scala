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


object TestCorrelation {
    val ssql = ConnectSparkSession.connect()
    

    def fireOnlyCorr(): Unit = {
        import ssql.implicits._
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



    def fireWeatherCorr(): Unit={
        import ssql.implicits._
        val fireDF = ssql.read.parquet("dataset-offline/train/randomSampleF0.0002.parquet").select("FIRE_SIZE", "LATITUDE",
        "LONGITUDE","FIRE_YEAR","DISCOVERY_DOY","CONT_DOY").filter("CONT_DOY is not NULL").filter("DISCOVERY_DOY is not NULL").filter("FIRE_SIZE is not NULL")
        var weather = ssql.read.csv("dataset-offline/train/randomSampleW0.0002.csv")
        var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk")
        val weatherDF1 = weatherDF.select("OBJECTID","tempmax", "tempmin", "dew", "humidity", "precip", "windspeed", "sealevelpressure", "cloudcover").withColumn("tempmax", col("tempmax").cast(DoubleType)).withColumn("tempmin", col("tempmin").cast(DoubleType)).withColumn("dew", col("dew").cast(DoubleType)).withColumn("humidity", col("humidity").cast(DoubleType)).withColumn("precip", 
        col("precip").cast(DoubleType)).withColumn("windspeed", col("windspeed").cast(DoubleType)).withColumn("sealevelpressure", col("sealevelpressure").cast(DoubleType)).withColumn("cloudcover", col("cloudcover").cast(DoubleType))
        val avgtempmax = weatherDF1.groupBy("OBJECTID").avg("tempmax")
        val avgtempmin = weatherDF1.groupBy("OBJECTID").avg("tempmin")
        val avgdew = weatherDF1.groupBy("OBJECTID").avg("dew")
        val avghumid = weatherDF1.groupBy("OBJECTID").avg("humidity")
        val avgprecip = weatherDF1.groupBy("OBJECTID").avg("precip")
        val avgwindsp = weatherDF1.groupBy("OBJECTID").avg("windspeed")
        val avgpress = weatherDF1.groupBy("OBJECTID").avg("sealevelpressure")
        val avgcloud = weatherDF1.groupBy("OBJECTID").avg("cloudcover")
        avgcloud.show()
        val a = avgtempmax.join(avgtempmin, avgtempmax("OBJECTID")===avgtempmin("OBJECTID"))
        
        //.join(avgdew, avgtempmax("OBJECTID")===avgdew("OBJECTID")).join(
            //avghumid, avgtempmax("OBJECTID")===avghumid("OBJECTID")).join(avgprecip, avgtempmax("OBJECTID")===avgprecip("OBJECTID"))
            .join(avgwindsp, avgtempmax("OBJECTID")===avgwindsp("OBJECTID")).join(
               avgpress, avgtempmax("OBJECTID")===avgpress("OBJECTID")).join(avgcloud, avgtempmax("OBJECTID")===avgcloud("OBJECTID"))
        weatherJoin.show()
        var corrArray = Seq(Vectors.dense(0,0,0))
        val joinFW = fireDF.join(weatherJoin, fireDF("OBJECTID")===weatherJoin("OBJECTID"), "inner").collect().foreach({row=>
            var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
            row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, row(13).toString.toDouble) 
            corrArray = corrArray :+ Vectors.dense(temp) 
        })
        print(corrArray.mkString)
        val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
        val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
        println("Pearson correlation matrix:\n" + coeff1.toString)
        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
        println("Spearman correlation matrix:\n" + coeff2.toString) 
    }   




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




