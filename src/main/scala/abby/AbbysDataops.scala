package abby

import contexts.ConnectSparkSession

object AbbysDataops {

  val ssql = ConnectSparkSession.connect()

  def runSummaryStatements() = {
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

    //UNDERNEATH IS MY DF SUMMARY STATEMENTS
    df_fire.groupBy("STATE").count().orderBy("count").show(1000)
  }
  
  // // parquet
  //val df = spark.read.option("multiline", true).parquet("input/fire1.parquet")

  // Code to generate .JSON or .Parquet Files from the 2.6GB json main file
  // need to create object outside main for SparkSession and import SparkSession
  def createJSONFile() = {
    val df_Wildfire = ssql.read.option("multiline","true").parquet("dataset/validation/fireComplete.parquet")
    var df_Wildfire_G = df_Wildfire.select(
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
      .filter(df_Wildfire("FIRE_SIZE_CLASS") === "G")
    

    df_Wildfire_G.write.parquet("dataset/train/fireG")
  }



//Need Unix_Timestamp_Seconds column generated from a udp on the two Wildfire columns FIRE_YEAR & DISCOVERY_DOY
def udp_Calc_Unix_Timestamp_Seconds(fire_year: Int, discover_doy: Int): Int = {
  val years_since_1970 = fire_year - 1970
  val SECONDS_IN_YEAR = 31557600
  val SECONDS_IN_DAY = 86400
  val Unix_Timestamp_Seconds = (SECONDS_IN_YEAR * years_since_1970) + (SECONDS_IN_DAY * discover_doy)
  Unix_Timestamp_Seconds
}
}