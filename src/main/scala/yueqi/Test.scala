package yueqi
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.StdIn.readLine
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.ml.linalg.Vectors
// import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel


object Test {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark1 = SparkSession.builder()
      .appName("WildFire")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark1.sparkContext.setLogLevel("ERROR")


    def getDayMonth(): Unit={

    import spark1.implicits._

    val df = spark1.read.option("multiline", true).parquet("dataset/fire1.parquet")
    df.createOrReplaceTempView("firetable")

    spark1.sql("SELECT * FROM firetable").show()

    val array = df.select("LATITUDE", "LONGITUDE", "FIRE_YEAR", "DISCOVERY_DOY").collect().foreach({row=>
    var lat = row(0).toString.toDouble
    var lon = row(1).toString.toDouble
    // 31,536,000 / 31,622,400 
    //{2005->1104559200, 2006->1136095200, 2007->1167631200, 2008->	1199167200, 2009->1230789600, 2010->1262325600, 
    //2011->1293861600, 2012->1325397600, 2013->1357020000, 2014->1388556000, 2015->1420092000}
    var dateMap = Map("2005"->1104559200, "2006"->1136095200, "2007"->1167631200, "2008"->1199167200, "2009"->1230789600, "2010"->1262325600, 
    "2011"->1293861600, "2012"->1325397600, "2013"->1357020000, "2014"->1388556000, "2015"->1420092000)
    //var unixSec = dateMap.getOrElse(row(2).toString, 0) +  86400 * row(3).toString.toInt
    var unixSec = 1104559200
    //println(unixSec)
    var start = unixSec-86400*3
    var end = unixSec+86400*3
    //var api = 
    var url = s"http://history.openweathermap.org/data/2.5/history/city?lat=$lat&lon=$lon&type=hour&start=$start&end=$end&appid=api"
    var tempData = scala.io.Source.fromURL(url).mkString
    println(tempData)
    
    //0c46283b46a5e97cd91006313f6f08d7
    })
    }
}

//geographic query for visualization: df entire_fire group by year, lattitude/longitude, fire size (200 classG/year, 400 classF/year, 600 classE/year...)
//df classG_fire join with weather on date(range) and lattitude/longitude to query weather condition for class G fire
//df fire join with weather on date(range) and latitude/longitude for query weather condition for class < C fires



/*two spark ways to read file: spark.read.csv/json (creates df) or spark.sparkContext.textFile (creates RDD)
    here I'm using RDD transformations to convert 54 columns of beverages each row representing a day's consumer counts into dense vectors
    the dense vectors will be fed into a feature extraction model from spark mllib */
    
    //creates a scala 'dictionary' of coffee names
    // var coffeedict: Map[String, Int]=Map()
    // var coffeeNames = Source.fromFile("input/CoffeeNames.txt")
    // var idx = 0
    // for (line<-coffeeNames.getLines) {
    //    coffeedict += (line -> idx)
    //    idx+=1
    // }
    // coffeeNames.close
    // var coffeeList = ArrayBuffer[Double]()
    // for(i<-0 to 54) {
    //   coffeeList+=0
    // }
    // var coffeeVector = Seq(Vectors.dense(0,0,0))
    // import spark1.implicits._
    // val rdd1 = spark1.sparkContext.textFile("input/CountACut.txt").map(line=>line.split(",")).map(line=>(line(0), line(1).toDouble)).reduceByKey(_+_)
    // rdd1.persist(StorageLevel.MEMORY_ONLY_SER)
    // rdd1.collect.foreach({ x=>
    //     var x0 = x._1
    //     var x1 = x._2
    //     var idx = coffeedict.getOrElse(x0, 0)
    //     coffeeList.update(idx, x1)
    //   })
    // rdd1.unpersist()
    // println("bryan is so sweet and helpful")
    // var v1 = Vectors.dense(coffeeList.toArray)
    // coffeeVector = coffeeVector :+ v1
    // println(coffeeVector.drop(1))

