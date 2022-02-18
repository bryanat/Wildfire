package dataops

import contexts.ConnectSparkSession
//import org.apache.spark.SparkContext._

object Sampling {

    val spark1 = ConnectSparkSession.connect()
    import spark1.implicits._

    def stratifiedSampling(): Unit={
    val df = spark1.read.option("multiline","true").parquet("dataset/validation/fireComplete.parquet")
    val fractions = Map("A"-> 0.1,"B"-> 0.2, "C"->0.4, "D"->0.6, "E"-> 0.8, "F"->0.9, "G"->0.99)
    val sample = df.stat.sampleBy("FIRE_SIZE_CLASS", fractions, 123)
    sample.write.parquet("dataset/train/sampled")

    }
  
}
