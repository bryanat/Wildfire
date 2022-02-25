package brandon
import contexts.ConnectSparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, udf}


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

 /*     // -----------------------------------------------------------------------------------------
      // BRANDON DF SUMMARIZE STATEMENTS BELOW
      //(1) Top 10 causes of Wildfires in the US
    //Key: 1 Lightning, 2 Equipment, 3 Smoking, 4 Campfire, 5 Debris Burning, 6 Railroad, 7 Arson, 8 Children, 9 Misc, 10 Firework, 11 Powerline, 12 Structure, 13 Missing/Undefined
    df_fire.groupBy("STAT_CAUSE_CODE").count().orderBy(col("FIRE_SIZE_CLASS")).withColumnRenamed("count", "US_TOTAL-FIRES_BY_CAUSE").show(10)
    //-----------
    // (2) Top three states with the highest number of fires from 1992-2015
    df_fire.groupBy("STATE").count().orderBy(col("count").desc).withColumnRenamed("count", "US_TOTAL-FIRES").show(3)
    //-----------
    // (3) CA,GA,TX: NUMBER OF FIRES BY CLASS (1992-2015)
    df_fire.where(df_fire("STATE") === "CA").groupBy("FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "CA_TOTAL-FIRES_BY_CLASS").show()
    df_fire.where(df_fire("STATE") === "GA").groupBy("FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "GA_TOTAL-FIRES_BY_CLASS").show()
    df_fire.where(df_fire("STATE") === "TX").groupBy("FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "TX_TOTAL-FIRES_BY_CLASS").show()
    // -----------
    // (4)  CA,GA,TX: CAUSES OF B-CLASS FIRES (1992-2015)
     df_fire.where(df_fire("STATE") === "CA" && df_fire("FIRE_SIZE_CLASS") === "B").groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS").count().withColumnRenamed("count", "CA_B-CLASS_BY_CAUSE").show()
     df_fire.where(df_fire("STATE") === "GA" && df_fire("FIRE_SIZE_CLASS") === "B").groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS").count().withColumnRenamed("count", "GA_B-CLASS_BY_CAUSE").show()
     df_fire.where(df_fire("STATE") === "TX" && df_fire("FIRE_SIZE_CLASS") === "B").groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS").count().withColumnRenamed("count", "TX_B-CLASS_BY_CAUSE").show()
    // -----------
    // (5) {most common cause across years}
    df_fire.where(df_fire("STAT_CAUSE_CODE") === "7.0").groupBy("STATE").count().orderBy(col("count").desc).withColumnRenamed("count", "US_ARSONS_BY_COUNT").show()
    // "FIRE_SIZE_CLASS"*, "FIRE_SIZE", "STATE"*, "COUNTY", "FIRE_YEAR", "STAT_CAUSE_CODE"*,
    // track arsons by state over the years
    df_fire.groupBy("STAT_CAUSE_CODE", "STATE", "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("STATE").desc).withColumnRenamed("count", "US_ARSONS_BY_STATE").show()
    // something more "spicy" and /or specific
    //track arsons
    df_fire.where(df_fire("STATE") === "CA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_YEAR").desc).withColumnRenamed("count", "CA_ARSONS_BY_CLASS").show(999)
    df_fire.where(df_fire("STATE") === "GA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_YEAR").desc).withColumnRenamed("count", "GA_ARSONS_BY_CLASS").show(999)
    df_fire.where(df_fire("STATE") === "TX" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS", "FIRE_YEAR").count().orderBy(col("FIRE_SIZE_CLASS").desc, col("FIRE_YEAR").desc).withColumnRenamed("count", "TX_ARSONS_BY_CLASS").show(999)
    //-------------VIZUALIZATION-FRIENDLY QUERIES--------------
    //CA QUERY (ARSON)
    df_fire.where(df_fire("STATE") === "CA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(   "FIRE_YEAR").count().orderBy(col("FIRE_YEAR").desc).withColumnRenamed("count", "CA_ARSONS_BY_YEAR").show(999)
    df_fire.where(df_fire("STATE") === "CA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "CA_ARSONS_BY_CLASS").show(999)
    //GA QUERY (ARSON)
    df_fire.where(df_fire("STATE") === "GA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(   "FIRE_YEAR").count().orderBy(col("FIRE_YEAR").desc).withColumnRenamed("count", "GA_ARSONS_BY_YEAR").show(999)
    df_fire.where(df_fire("STATE") === "GA" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "GA_ARSONS_BY_CLASS").show(999)
    //TX QUERY (ARSON)
    df_fire.where(df_fire("STATE") === "TX" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(   "FIRE_YEAR").count().orderBy(col("FIRE_YEAR").desc).withColumnRenamed("count", "TX_ARSONS_BY_YEAR").show(999)
    df_fire.where(df_fire("STATE") === "TX" && df_fire("STAT_CAUSE_CODE") === "7.0").groupBy(  "FIRE_SIZE_CLASS").count().orderBy(col("FIRE_SIZE_CLASS").desc).withColumnRenamed("count", "TX_ARSONS_BY_CLASS").show(999)
    // ------------------------------------------------------------------------
*/

    //BROADCAST
    val df_StatePopulation = ssql.read.option("multiline","true").json("dataset/statepopulation.json")
    //df_StatePopulation.show()
    ssql.sparkContext.broadcast(df_StatePopulation)
    //BROADCAST JOIN to add population column by state
   var df_broadcastJoined = df_fire.join(df_StatePopulation, df_fire("STATE") === df_StatePopulation("State")).drop(df_fire("STATE"))
   val df_arson = df_fire.where(df_fire("STAT_CAUSE_CODE") === "7.0").groupBy("STATE").count().withColumnRenamed("STATE", "States")
    //create lambda func as a udf
    val arsonIndexUDF = udf((arson : Int, population : Int) => {
    // arsonIndex is arson per capita normalized to two digits with * 10,000
      val arsonIndex = (arson.toFloat / population)*10000
      arsonIndex
    })
      //add arson column
    df_broadcastJoined.join(df_arson, df_broadcastJoined("STATE") === df_arson("States"))
      .select(df_broadcastJoined("STATE"), arsonIndexUDF(df_arson("count"), df_broadcastJoined("Population")).alias("ArsonIndex_UDF"))
      .distinct().orderBy(col("ArsonIndex_UDF").desc).show(50)

  }

}
