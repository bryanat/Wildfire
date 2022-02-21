package dataops

import contexts.ConnectSparkSession

object QueryFire {

  val ssql = ConnectSparkSession.connect()

  // Code to generate .JSON or .Parquet Files from the 2.6GB json main file
  // need to create object outside main for SparkSession and import SparkSession
  def queryTexas() = {
    val df_Wildfire = ssql.read.option("multiline","true").parquet("dataset-online/train/WildfireAll.parquet")
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

    // var df_Wildfire_Texas = df_Wildfire_G.where(df_Wildfire_G("STATE") === "TX").groupBy(df_Wildfire_G("FIRE_SIZE_CLASS")).show()
    var df_Wildfire_Texas = df_Wildfire_G.where(df_Wildfire_G("STATE") === "TX").show()

  }
    
  def queryAlaska() = {

  }

    //df_Wildfire_G.write.parquet("dataset-offline/train/fireALL")
}