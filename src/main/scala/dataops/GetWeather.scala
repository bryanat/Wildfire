package dataops
import scala.io.Source
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import java.io._ 
import java.time._



object GetWeather {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val ssql = SparkSession.builder()
      .appName("WildFire")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    ssql.sparkContext.setLogLevel("ERROR")


    def getWeather(readFile: String, writeFile: String): Unit={

    import ssql.implicits._
    val df = ssql.read.option("multiline", true).parquet(readFile)
    var weather_ListBuffer = ListBuffer[String]()
    try{
      val array = df.select("OBJECTID", "LATITUDE", "LONGITUDE","FIRE_YEAR", "DISCOVERY_DOY", "CONT_DOY").collect().foreach({row=>
      var id= row(0).toString
      try{
        var lat = row(1).toString.toDouble
        var lon = row(2).toString.toDouble
        var year = row(3).toString
        if (row(4).toString.toInt-14>0) {
        var start = dateConversion(row(3).toString, row(4).toString.toInt-14)
        var end = dateConversion(row(3).toString, row(4).toString.toInt)
        // var start = dateConversion(row(3).toString, row(4).toString.toInt)
        // var end = dateConversion(row(3).toString, row(5).toString.toInt)
        if (end<start) {
          end= dateConversion((row(3).toString.toInt+1).toString, row(5).toString.toInt)
        }
        //To handle nullpointerexception error, if cont_doy null, set cont_doy to discovery_doy
        //var end = ""
        // if (row(5)==null) {
        //   end = start
        
        // }
        // else{
        //   end = dateConversion(row(3).toString, row(5).toString.toInt)
        // }
        var url = s"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/$lat%2C$lon/$start/$end?unitGroup=metric&include=days&key=SVV5ZE8VC54DVH27W4ZMAVFMR&contentType=csv"    
        var bufferedSource = scala.io.Source.fromURL(url)
        var first = 1
        for (line <- bufferedSource.getLines) {
          if (first!=1) {
          var cols = line.split(",").map(_.trim)
          weather_ListBuffer += s"${id},${cols(0)},${cols(1)},${cols(2)},${cols(3)},${cols(4)},${cols(5)},${cols(6)},${cols(7)},${cols(8)},${cols(9)},${cols(10)},${cols(11)},${cols(12)},${cols(13)},${cols(14)},${cols(15)},${cols(16)},${cols(17)},${cols(18)},${cols(19)},${cols(20)},${cols(21)},${cols(22)},${cols(23)},${cols(24)},${cols(25)},${cols(26)}"
        }
        first+=1
        }
        bufferedSource.close
      }
      }
      catch {
        case e: NullPointerException => println(s"$id fire skipped")
      }
    })
  }
    catch {
        case e: NullPointerException => println("fire skipped outer")
      }
    val weatherList = weather_ListBuffer.toList
    writeWeather(writeFile, weatherList)
    }


    def writeWeather(writeFile: String, weatherList: List[String]): Unit = {
    val file = new File(writeFile)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- weatherList) {
        bw.write(line)
        bw.write("\n")
    }
    bw.close()
    }

    def dateConversion(year:String, doy:Int): String={
      var start = year+"-01-01"
      var time = LocalDate.parse(start)
      var firedate = time.plus(Period.ofDays(doy-1))
      return firedate.toString
    }
}



