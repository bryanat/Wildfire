package yueqi
import CorrelationMatrixOps._
import contexts.ConnectSparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.ml.param.ParamMap


// ultimate goal: predict future chances of wildfires given temperatures

object LogRegressionOps {
     val ssql = ConnectSparkSession.connect()
    import ssql.implicits._

  def fitClassAndWeather(): Unit={
      val rowArrayTrain = fireWeatherCorr("dataset-online/train/stratifiedSampleF2.parquet", "dataset-online/train/stratifiedSampleW2.csv")
      var corrArrayTrain = Seq(Tuple2(0.toDouble, Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)))
      rowArrayTrain.foreach({row=>
        //find the fire_size_class from ID
        var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, 
                row(13).toString.toDouble, row(14).toString.toDouble, row(15).toString.toDouble, row(16).toString.toDouble) 
            corrArrayTrain = corrArrayTrain :+ Tuple2(row(0).toString.toDouble, Vectors.dense(temp))
        })
        val training = corrArrayTrain.drop(1).toDF("label", "features")
        training.show()
        // Create a LogisticRegression instance. This instance is an Estimator.
        val lr = new LogisticRegression()
        // Print out the parameters, documentation, and any default values.
        println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

        // We may set parameters using setter methods.
        lr.setMaxIter(10)
          .setRegParam(0.01)

          // Learn a LogisticRegression model. This uses the parameters stored in lr.
        val model1 = lr.fit(training)
        println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

        // We may alternatively specify parameters using a ParamMap,
        // which supports several methods for specifying parameters.
        val paramMap = ParamMap(lr.maxIter -> 20)
          .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
          .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

        // One can also combine ParamMaps.
        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
        val paramMapCombined = paramMap ++ paramMap2

        // Now learn a new model using the paramMapCombined parameters.
        // paramMapCombined overrides all parameters set earlier via lr.set* methods.
        val model2 = lr.fit(training, paramMapCombined)
        println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)
        val rowArrayTest = fireWeatherCorr("dataset-online/train/stratifiedSampleF2.parquet", "dataset-online/train/stratifiedSampleW2.csv")
        var corrArrayTest = Seq(Tuple2(0.toDouble, Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)))
        rowArrayTest.foreach({row=>
        //find the fire_size_class from ID
       var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble,row(12).toString.toDouble, 
                row(13).toString.toDouble, row(14).toString.toDouble, row(15).toString.toDouble, row(16).toString.toDouble) 
            corrArrayTest = corrArrayTest :+ Tuple2(row(0).toString.toDouble, Vectors.dense(temp))
        })
        val test = corrArrayTest.drop(1).toDF("label", "features")
        model2.transform(test)
          .select("features", "label", "myProbability", "prediction")
          .collect()
          .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
            println(s"($features, $label) -> prob=$prob, prediction=$prediction")
          }



      }


  }

/*

// Create a LogisticRegression instance. This instance is an Estimator.
val lr = new LogisticRegression()
// Print out the parameters, documentation, and any default values.
println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

// We may set parameters using setter methods.
lr.setMaxIter(10)
  .setRegParam(0.01)

// Learn a LogisticRegression model. This uses the parameters stored in lr.
val model1 = lr.fit(training)
// Since model1 is a Model (i.e., a Transformer produced by an Estimator),
// we can view the parameters it used during fit().
// This prints the parameter (name: value) pairs, where names are unique IDs for this
// LogisticRegression instance.
println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

// We may alternatively specify parameters using a ParamMap,
// which supports several methods for specifying parameters.
val paramMap = ParamMap(lr.maxIter -> 20)
  .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
  .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

// One can also combine ParamMaps.
val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
val paramMapCombined = paramMap ++ paramMap2

// Now learn a new model using the paramMapCombined parameters.
// paramMapCombined overrides all parameters set earlier via lr.set* methods.
val model2 = lr.fit(training, paramMapCombined)
println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

*/
