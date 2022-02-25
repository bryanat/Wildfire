package yueqi
import contexts.ConnectSparkSession
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.Row

//the chi square test determines whether there is a strong correlation between categorical features tested against a label

object Chisquare {
  val ssql = ConnectSparkSession.connect()
  import ssql.implicits._



  def fireCauseChi(): Unit = {
    var fireVector = Seq(Tuple2(0.0, Vectors.dense(0,0,0)))
    val file = ssql.read.parquet("dataset-online/train/stratifiedSampleF2.parquet")
    val statesMap = ssql.read.csv("dataset-offline/validation/statemap.csv").toDF("state", "id").select($"state", $"id".cast("int")).as[(String, Int)].collect.toMap
    var Afire = ArrayBuffer[Double]()
    var Bfire = ArrayBuffer[Double]()
    var Cfire = ArrayBuffer[Double]()
    var Dfire = ArrayBuffer[Double]()
    var Efire = ArrayBuffer[Double]()
    var Ffire = ArrayBuffer[Double]()
    var Gfire = ArrayBuffer[Double]()
    for (i<-0 to 12) {
      Afire+=0
    }
    for (i<-0 to 12) {
      Bfire+=0
    }
    for (i<-0 to 12) {
      Cfire+=0
    }
    for (i<-0 to 12) {
      Dfire+=0
    }
    for (i<-0 to 12) {
      Efire+=0
    }
    for (i<-0 to 12) {
      Ffire+=0
    }
    for (i<-0 to 12) {
      Gfire+=0
    }
    file.select("FIRE_SIZE_CLASS", "STAT_CAUSE_CODE", "FIRE_YEAR", "STATE").collect.foreach({row=>
        var fireclass = row(0).toString
        var cause = row(1).toString.toDouble
        var idx = cause.toDouble.toInt-1
        if (fireclass=="A") {
          var value = Afire(idx)+1
          Afire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(0, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="B") {
          var value = Bfire(idx)+1
          Bfire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(1, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="C") {
           var value = Cfire(idx)+1
          Cfire.update(idx, value)
        //   fireVector = fireVector :+ Tuple2(2, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="D") {
          var value = Dfire(idx)+1
          Dfire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(3, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="E") {
           var value = Efire(idx)+1
          Efire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(4, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="F") {
           var value = Ffire(idx)+1
          Ffire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(5, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else{
           var value = Gfire(idx)+1
          Gfire.update(idx, value)
        //   fireVector = fireVector :+ Tuple2(6, Vectors.sparse(13, Array(idx), Array(1)))
        }
      })
    val Avector = Tuple2(0.0, Vectors.dense(Afire.toArray))
    val Bvector = Tuple2(1.0, Vectors.dense(Bfire.toArray))
    val Cvector = Tuple2(2.0, Vectors.dense(Cfire.toArray))
    val Dvector = Tuple2(3.0, Vectors.dense(Dfire.toArray))
    val Evector = Tuple2(4.0, Vectors.dense(Efire.toArray))
    val Fvector = Tuple2(5.0, Vectors.dense(Ffire.toArray))
    val Gvector = Tuple2(6.0, Vectors.dense(Gfire.toArray))
    fireVector = fireVector :+ Avector
    fireVector = fireVector :+ Bvector
    fireVector = fireVector :+ Cvector
    fireVector = fireVector :+ Dvector
    fireVector = fireVector :+ Evector
    fireVector = fireVector :+ Fvector
    fireVector = fireVector :+ Gvector
    println(fireVector.mkString)
    val df = fireVector.drop(1).toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
  
    // println(s"pValues = ${chi.getAs[Vector](0)}")
    // println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    // println(s"statistics ${chi.getAs[Vector](2)}")

  }





//null hypothesis: fire size class and cause of fires are unrelated
  def fireStateChi(): Unit ={

    var fireVector = Seq(Tuple2(0.0, Vectors.dense(0,0,0)))
    val file = ssql.read.parquet("dataset-online/train/stratifiedSampleF2.parquet")
    val statesMap = ssql.read.csv("dataset-offline/validation/statemap.csv").toDF("state", "id").select($"state", $"id".cast("int")).as[(String, Int)].collect.toMap
    val classNumMap = Map("A"->0, "B"->1, "C"->2, "D"->3, "E"->4, "F"->5, "G"->6) 
    var Afire = ArrayBuffer[Double]()
    var Bfire = ArrayBuffer[Double]()
    var Cfire = ArrayBuffer[Double]()
    var Dfire = ArrayBuffer[Double]()
    var Efire = ArrayBuffer[Double]()
    var Ffire = ArrayBuffer[Double]()
    var Gfire = ArrayBuffer[Double]()
    for (i<-0 to 51) {
      Afire+=0
    }
    for (i<-0 to 51) {
      Bfire+=0
    }
    for (i<-0 to 51) {
      Cfire+=0
    }
    for (i<-0 to 51) {
      Dfire+=0
    }
    for (i<-0 to 51) {
      Efire+=0
    }
    for (i<-0 to 51) {
      Ffire+=0
    }
    for (i<-0 to 51) {
      Gfire+=0
    }
    file.select("FIRE_SIZE_CLASS", "STAT_CAUSE_CODE", "FIRE_YEAR", "STATE").collect.foreach({row=>
        var fireclass = row(0).toString
        var state = row(3).toString
        var stateCode =  statesMap.getOrElse(state, 0).toDouble
        var idx = stateCode.toInt-1
        if (fireclass=="A") {
          var value = Afire(idx)+1
          Afire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(0, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="B") {
          var value = Bfire(idx)+1
          Bfire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(1, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="C") {
           var value = Cfire(idx)+1
          Cfire.update(idx, value)
        //   fireVector = fireVector :+ Tuple2(2, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="D") {
          var value = Dfire(idx)+1
          Dfire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(3, Vectors.sparse(13, Array(idx), Array(1)))
         }
        else if (fireclass=="E") {
           var value = Efire(idx)+1
          Efire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(4, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else if (fireclass=="F") {
           var value = Ffire(idx)+1
          Ffire.update(idx, value)
        //     fireVector = fireVector :+ Tuple2(5, Vectors.sparse(13, Array(idx), Array(1)))
        }
        else{
           var value = Gfire(idx)+1
          Gfire.update(idx, value)
        //   fireVector = fireVector :+ Tuple2(6, Vectors.sparse(13, Array(idx), Array(1)))
        }
      })
    val Avector = Tuple2(0.0, Vectors.dense(Afire.toArray))
    val Bvector = Tuple2(1.0, Vectors.dense(Bfire.toArray))
    val Cvector = Tuple2(2.0, Vectors.dense(Cfire.toArray))
    val Dvector = Tuple2(3.0, Vectors.dense(Dfire.toArray))
    val Evector = Tuple2(4.0, Vectors.dense(Efire.toArray))
    val Fvector = Tuple2(5.0, Vectors.dense(Ffire.toArray))
    val Gvector = Tuple2(6.0, Vectors.dense(Gfire.toArray))
    fireVector = fireVector :+ Avector
    fireVector = fireVector :+ Bvector
    fireVector = fireVector :+ Cvector
    fireVector = fireVector :+ Dvector
    fireVector = fireVector :+ Evector
    fireVector = fireVector :+ Fvector
    fireVector = fireVector :+ Gvector
    println(fireVector.mkString)
    val df = fireVector.drop(1).toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }
}
