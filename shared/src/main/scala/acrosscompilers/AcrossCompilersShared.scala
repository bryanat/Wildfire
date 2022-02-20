package acrosscompilers

import contexts.ConnectSparkSession

object AcrossCompilersShared {

  // Transfers a dataframe as an array, primarily containing Latitude, Longitude and ObjectId
  def transferCoords() = {
    val ssql = ConnectSparkSession.connect()
    val df_Wildfire = ssql.read.option("multiline", "true").parquet("dataset-online/train/WildfireAll.parquet")
    var df_Wildfire_G = df_Wildfire.select(
      "FIRE_SIZE_CLASS",
      "FIRE_SIZE",
      "LATITUDE",
      "LONGITUDE",
      "OBJECTID")
      .limit(100)
    var array_Wildfire_G = df_Wildfire_G.collect()
    array_Wildfire_G
  }

}
