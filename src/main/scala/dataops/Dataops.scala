package dataops

import contexts.ConnectSparkSession

object Dataops {

  val ssql = ConnectSparkSession.connect()

  // Code to generate .JSON or .Parquet Files from the 2.6GB json main file
  // need to create object outside main for SparkSession and import SparkSession
  def createJSONFile() = {
    val df_Wildfire = ssql.read.option("multiline","true").json("dataset/Fires.json")
    var df_Wildfire_G = df_Wildfire.select("OBJECTID", "FIRE_SIZE_CLASS", "LONGITUDE", "LATITUDE", "FIRE_YEAR", "DISCOVERY_DOY").filter(df_Wildfire("FIRE_SIZE_CLASS") === "G")
    df_Wildfire_G.write.json("dataset/WildfireG5")
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