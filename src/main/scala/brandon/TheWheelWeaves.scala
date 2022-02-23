package brandon
import contexts.ConnectSparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col


object TheWheelWeaves {
  def asTheWheelWills(): Unit = {

    val ssql = ConnectSparkSession.connect()

    // // parquet
    //val df = spark.read.option("multiline", true).parquet("input/fire1.parquet")

    // Code to generate .JSON or .Parquet Files from the 2.6GB json main file
    // need to create object outside main for SparkSession and import SparkSession
      val df_Wildfire = ssql.read.option("multiline","true").parquet("dataset/WildfireAll.parquet")
      var df_fire = df_Wildfire.select(
        "FIRE_SIZE_CLASS",
        "FIRE_SIZE",
        "LATITUDE",
        "LONGITUDE",
        "STATE",
        "COUNTY",
        "NWCG_REPORTING_UNIT_NAME",
        "FIRE_YEAR",
        "DISCOVERY_DOY",
        "CONT_DOY",
        "STAT_CAUSE_CODE",
        "FIRE_NAME",
        "OBJECTID")

      // -----------------------------------------------------------------------------------------
      // BRANDON DF SUMMARIZE STATEMENTS BELOW
      //(1) Top 10 causes of Wildfires in the US
    //Key: 1 Lightning, 2 Equipment, 3 Smoking, 4 Campfire, 5 Debris Burning, 6 Railroad, 7 Arson, 8 Children, 9 Misc, 10 Firework, 11 Powerline, 12 Structure, 13 Missing/Undefined
    df_fire.groupBy("STAT_CAUSE_CODE").count().show(10)
    //-----------
    // (2) Top three states with the highest number of fires from 1992-2015
    df_fire.groupBy("STATE").count().orderBy(col("count").desc).show(3)
    //-----------
    // (3) CA,GA,TX: NUMBER OF FIRES BY CLASS (1992-2015)
    df_fire.where(df_fire("STATE") === "CA").groupBy("FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).show()
    df_fire.where(df_fire("STATE") === "GA").groupBy("FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).show()
    df_fire.where(df_fire("STATE") === "TX").groupBy("FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).show()
    // -----------
    // (4)  CA,GA,TX: CAUSES OF B-CLASS FIRES (1992-2015)
     df_fire.where(df_fire("STATE") === "CA" && df_fire("FIRE_SIZE_CLASS") === "B").groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS").count().show()
     df_fire.where(df_fire("STATE") === "GA" && df_fire("FIRE_SIZE_CLASS") === "B").groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS").count().show()
     df_fire.where(df_fire("STATE") === "TX" && df_fire("FIRE_SIZE_CLASS") === "B").groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS").count().show()
    // -----------
    // (5) {most common cause across years}
    df_fire.where(df_fire("STAT_CAUSE_CODE") === "7.0").groupBy("STATE").count().orderBy(col("count").desc).show()
    // "FIRE_SIZE_CLASS"*, "FIRE_SIZE", "STATE"*, "COUNTY", "FIRE_YEAR", "STAT_CAUSE_CODE"*,
    // track arsons by state over the years
    df_fire.groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("STATE").desc).show()
    // something more "spicy" and /or specific
    //track arsons
    df_fire.where(df_fire("STATE") === "CA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_YEAR").desc).show(999)
    df_fire.where(df_fire("STATE") === "GA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_YEAR").desc).show(999)
    //keep
    df_fire.where(df_fire("STATE") === "TX" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_YEAR").desc).show(999)
    //-------------VIZUALIZATION-FRIENDLY QUERIES--------------
    //CA QUERY (ARSON)
    df_fire.where(df_fire("STATE") === "CA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(   "FIRE_YEAR").count().orderBy(col("FIRE_YEAR").desc).withColumnRenamed("count", "TX_ARSONS_BY_YEAR").show(999)
    df_fire.where(df_fire("STATE") === "CA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "TX_ARSONS_BY_CLASS").show(999)
    //GA QUERY (ARSON)
    df_fire.where(df_fire("STATE") === "GA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(   "FIRE_YEAR").count().orderBy(col("FIRE_YEAR").desc).withColumnRenamed("count", "TX_ARSONS_BY_YEAR").show(999)
    df_fire.where(df_fire("STATE") === "GA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "TX_ARSONS_BY_CLASS").show(999)
    //TX QUERY (ARSON)
    df_fire.where(df_fire("STATE") === "TX" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(   "FIRE_YEAR").count().orderBy(col("FIRE_YEAR").desc).withColumnRenamed("count", "TX_ARSONS_BY_YEAR").show(999)
    df_fire.where(df_fire("STATE") === "TX" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "TX_ARSONS_BY_CLASS").show(999)

  }

}
