package yueqi
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest

//the chi square test determines whether there is a strong correlation between categorical features tested against a label

object Chisquare {


  def fireSizeAndState(): Unit={

  }


  def fireSizeAndCause(): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark1 = SparkSession.builder()
      .appName("WildFire")
      .config("spark.master", "local") 
      .enableHiveSupport()
      .getOrCreate()

    // are fire size and cause correlated

    
    var smallFire = ArrayBuffer[Double]()
    var bigFire = ArrayBuffer[Double]()
    for(i<-0 to 13) {
       smallFire+=0
     }
    for(i<-0 to 13) {
       bigFire+=0
     }
    //labels: class A-G
    //features: causes 1-13
    var fireVector =  Seq(Tuple2(1, Vectors.dense(0,0,0)))
    val file = spark1.read.parquet("dataset/validation/fireComplete.parquet")
    file.show()
    file.select("FIRE_SIZE_CLASS", "STAT_CAUSE_CODE").collect.foreach({row=>
        var fireclass = row(0).toString
        var cause = row(1).toString
        var idx = cause.toDouble.toInt-1
        if (fireclass=="A") {
            fireVector = fireVector :+ Tuple2(0, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="B") {
            fireVector = fireVector :+ Tuple2(1, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="C") {
          fireVector = fireVector :+ Tuple2(2, Vectors.sparse(13, Array(idx), Array(1)))
        }
         else if (fireclass=="D") {
            fireVector = fireVector :+ Tuple2(3, Vectors.sparse(13, Array(idx), Array(1)))
        }
         else if (fireclass=="E") {
            fireVector = fireVector :+ Tuple2(4, Vectors.sparse(13, Array(idx), Array(1)))
        }
         else if (fireclass=="F") {
            fireVector = fireVector :+ Tuple2(5, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else{
          fireVector = fireVector :+ Tuple2(6, Vectors.sparse(13, Array(idx), Array(1)))
        }
        })
    // file.select("FIRE_SIZE",  "STAT_CAUSE_CODE").collect.foreach({row=>
    //     var size = row(0).toString
    //     var cause = row(1).toString
    //     var idx = cause.toDouble.toInt-1
    //     if (size.toDouble < 10000) {
    //         smallFire.update(idx, smallFire(idx)+1)
    //     }
    //     else {
    //         bigFire.update(idx, bigFire(idx)+1)           
    //     }
    // })
    // fireVector = fireVector :+ Tuple2(0, Vectors.dense(smallFire.toArray))
    // fireVector = fireVector :+ Tuple2(1, Vectors.dense(bigFire.toArray))
    // print(fireVector.mkString)
    import spark1.implicits._
    val df = fireVector.drop(1).toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }
}
