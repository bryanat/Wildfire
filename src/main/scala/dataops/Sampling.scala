package dataops

import contexts.ConnectSparkSession

object Sampling {

    val spark1 = ConnectSparkSession.connect()
    import spark1.implicits._

    def stratifiedSampling(readFile:String, writeFile:String): Unit={
        val df = spark1.read.option("multiline","true").parquet(readFile)
        //fractions is a map that specifies which percentage of classA fire to classG fire you want; it picks a percentage of sample for each key
        val fractions = Map("A"-> 0.01,"B"-> 0.05, "C"->0.1, "D"->0.2, "E"-> 0.4, "F"->0.5, "G"->0.8)
        //123 is the seed: if you want the same sample next time, use 123 again; if you want a different sample, use another seed: 456, 1234, 78, anything works. 
        val sample = df.stat.sampleBy("FIRE_SIZE_CLASS", fractions, 13)
        sample.write.parquet(writeFile)
    }

    def randomSampling(readFile:String, writeFile:String): Unit={
        val df = spark1.read.option("multiline", "true").parquet(readFile)
        //unlike the fractions in stratified sampling for each partition, the fraction in random sampling is applied to the entire dataset
        var fraction = 0.0003
        //you can specify an optional seed after fraction
        val sample = df.sample(fraction)
        sample.write.parquet(writeFile)
    }
  
}
