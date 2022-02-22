package yueqi
import contexts.ConnectSparkSession
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest

//the chi square test determines whether there is a strong correlation between categorical features tested against a label

object Chisquare {
  val ssql = ConnectSparkSession.connect()
  import ssql.implicits._


//null hypothesis: fire size class and cause of fires are unrelated
  def fireChi(): Unit ={
    // var smallFire = ArrayBuffer[Double]()
    // var bigFire = ArrayBuffer[Double]()
    // for(i<-0 to 13) {
    //    smallFire+=0
    //  }
    // for(i<-0 to 13) {
    //    bigFire+=0
    //  }
    // labels: class A-G
    //features: causes 1-13
    //var fireVector =  Seq(Tuple2(1, Vectors.dense(0,0,0)))
    var fireVector = Seq(Tuple2(0, Vectors.dense(0,0,0)))
    val file = ssql.read.parquet("dataset-offline/train/stratifiedSampleAll3.parquet")
    val statesMap = ssql.read.csv("dataset-offline/validation/statemap.csv").toDF("state", "id").select($"state", $"id".cast("int")).as[(String, Int)].collect.toMap
    val classNumMap = Map("A"->0, "B"->1, "C"->2, "D"->3, "E"->4, "F"->5, "G"->6) 
    file.select("FIRE_SIZE_CLASS", "STAT_CAUSE_CODE", "FIRE_YEAR", "STATE").collect.foreach({row=>
        var fireclass = row(0).toString
        var cause = row(1).toString.toDouble
        //var idx = cause.toDouble.toInt-1
        var year = row(2).toString.toDouble
        var state = row(3).toString
        var stateCode =  statesMap.getOrElse(state, 0).toDouble
        if (fireclass=="A"|fireclass=="B"|fireclass=="C") {
          fireVector = fireVector :+  Tuple2(0,Vectors.dense(Array(cause, year, stateCode)))
          //fireVector = fireVector :+ Tuple2(0, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="D"|fireclass=="E"|fireclass=="F") {
          fireVector = fireVector :+  Tuple2(1,Vectors.dense(Array(cause, year, stateCode)))
            //fireVector = fireVector :+ Tuple2(1, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="G") {
          fireVector = fireVector :+  Tuple2(2,Vectors.dense(Array(cause, year, stateCode)))
            //fireVector = fireVector :+ Tuple2(2, Vectors.sparse(13, Array(idx), Array(1)))
        }
      })
        // if (fireclass=="A") {
        //     fireVector = fireVector :+ Tuple2(0, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        // else if (fireclass=="B") {
        //     fireVector = fireVector :+ Tuple2(1, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        // else if (fireclass=="C") {
        //   fireVector = fireVector :+ Tuple2(2, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        //  else if (fireclass=="D") {
        //     fireVector = fireVector :+ Tuple2(3, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        //  else if (fireclass=="E") {
        //     fireVector = fireVector :+ Tuple2(4, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        //  else if (fireclass=="F") {
        //     fireVector = fireVector :+ Tuple2(5, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        // else{
        //   fireVector = fireVector :+ Tuple2(6, Vectors.sparse(13, Array(idx), Array(1)))
        // }
        // })
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
    val df = fireVector.drop(1).toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }
}
