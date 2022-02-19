package yueqi

import contexts.ConnectSparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row




object LogRegression {

  def fireSizeAndWeather(): Unit={
      val ssql = ConnectSparkSession.connect()
    //avg tempmax, avg tempmin, avg windspeed, avg humidity, avg dew, avg precip, avg sealevelpressure, avg cloudcover, avg precip
      val firedf = ssql.read.parquet("dataset-offline/train/stratifiedSampleAll2.parquet")
      var weather = ssql.read.csv("dataset-offline/train/testweather3.csv")
      var weatherdf = weather.toDF("OBJECTID","name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover","preciptype","snow","snowdepth","windgust","windspeed","winddir",
  "sealevelpressure","cloudcover","visibility","solarradiation","solarenergy","uvindex","severerisk","sunrise","sunset","moonphase","conditions").filter("windspeed is not NULL").filter("humidity is not NULL").filter("precip is" +
    " not NULL").filter("dew is not NULL").filter("sealevelpressure is not NULL").filter("cloudcover is not NULL").filter("tempmax is not NULL").filter("tempmin is not NULL").filter("temp is not NULL")
      

      // Prepare training data from a list of (label, features) tuples.
// val training = spark.createDataFrame(Seq(
//   (1.0, Vectors.dense(0.0, 1.1, 0.1)),
//   (0.0, Vectors.dense(2.0, 1.0, -1.0)),
//   (0.0, Vectors.dense(2.0, 1.3, 1.0)),
//   (1.0, Vectors.dense(0.0, 1.2, -0.5))
// )).toDF("label", "features")




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



}

}