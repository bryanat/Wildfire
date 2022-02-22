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



    def pearsonCorr(arrayWF:Array[Row], fireFile:String, weatherFile:String): Unit = {
        def defPearsonCorr(arrayWF:Array[Row]): Unit = {
            var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0))
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
            var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0))
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

//0.Fire_Size_Class 1. Fire_Size 2. Lattitude 3. Longitude 4. Fire_year 5. Discovery_doy 6. Cont_doy 7. avgtempmax 8. avgtempmin 9. avgdew 10. avghumid 11. avgprecip 12. avgwindsp 13. avgpress 14. avgcloud
    def fireWeatherCorr(fireFile: String, weatherFile:String): Array[Row]={
        val fireNumMap = Map("A"->0, "B"->1, "C"->2, "D"->3, "E"->4, "F"->5, "G"->6)  
        val classudf = udf((fireclass: String)=>fireNumMap.get(fireclass))
        val burndaysudf = udf((startdate:Int, enddate:Int)=>enddate-startdate)
        val fireDF = ssql.read.parquet(fireFile).select($"OBJECTID", classudf($"FIRE_SIZE_CLASS"),$"FIRE_SIZE", $"LATITUDE",
        $"LONGITUDE",$"FIRE_YEAR",burndaysudf($"DISCOVERY_DOY", $"CONT_DOY")).filter("CONT_DOY is not NULL").filter("DISCOVERY_DOY is not NULL").filter("FIRE_SIZE is not NULL").filter("FIRE_YEAR is not NULL")
        var weather = ssql.read.csv(weatherFile)
        var weatherDF = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
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
        val weatherJoin = avgtempmax.join(avgtempmin, avgtempmax("OBJECTID1")===avgtempmin("OBJECTID2")).drop("OBJECTID1")
            .join(avgdew, avgtempmin("OBJECTID2")===avgdew("OBJECTID3")).drop("OBJECTID2")
            .join(avghumid, avgdew("OBJECTID3")===avghumid("OBJECTID4")).drop("OBJECTID3")
            .join(avgprecip, avghumid("OBJECTID4")===avgprecip("OBJECTID5")).drop("OBJECTID4")
            .join(avgwindsp, avgprecip("OBJECTID5")===avgwindsp("OBJECTID6")).drop("OBJECTID5")
             .join(avgpress, avgwindsp("OBJECTID6")===avgpress("OBJECTID7")).drop("OBJECTID6")
             .join(avgcloud, avgpress("OBJECTID7")===avgcloud("OBJECTID8")).drop("OBJECT7")
        //drop objectid on column ~12
        val joinFW = fireDF.join(weatherJoin, fireDF("OBJECTID")===weatherJoin("OBJECTID8")).drop("OBJECTID8").drop("OBJECTID").collect()
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




