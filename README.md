# Wildfire (P2 Spark)

### Project Members (Group A)
- Bryan Boyett
- Yueqi Peng
- Abby Harrah
- Brandon Conover

# Tech
- Spark
  - RDD, Dataset, Dataframe
- Spark Streaming
  - DStream
- Spark MLlib
  - Vector
- Spark SQL
  - Dataframe
- D3.js 
  - Scala.js crossbuild

# Dataset
- Wildfire (1.88 Million US Wildfires @Kaggle)
  - https://www.kaggle.com/rtatman/188-million-us-wildfires
- Weather API 
- Wildfire<>Time<>>Weather 
  - Connect Wildfire dataset with Weather API using unix Timestamp to connect the two datasets (like a foreign key)
  - Timestamp provides granularity down to seconds but can simplify to daily granularity, as no real value gained from minutes overs daily 

# P2 Primary Objective
- ## Summarize Query Layer
- Create a Spark Application that process data(choose your data).
- Size of data should be 2k and above,and a minimum 3 tables.
- Your project  should involve useful analysis of data(Every concept of spark like rdd, dataframes, sql, dataset and optimization methods  and  persistence should be included). The Expected output is different trends that you have observed as part of data collectivley and how you can use these trends to make some useful decisions.
-Should have admin and normal user access with password set in database along with Visualization  for out put 
-Let the P2, have presentation with screen shots and practical demo.

# P2 Secondary Objective
- ## Analytics Layer
- MLlib for Analytical Trends in the Wildfire+Weather data
- notable weather data: Humidity, Temperature, Precipitation/Rainfall, Pressure,
- geographical distribution of fires
  - example: Class G fires (5,000+ acre fires) are dominantly in California
  - what is it about California that predisposes it to Class G? Drought (Precipitation/Rainfall), Humidity, Temperature, ???

# P2 Tertiary Objective
- ## Visualization Layer
- D3.js for US Map Geographic Coordinates 
- Timestamp column for visualization over time as if Realtime
- Lat & Lon columns for Geographic Coordinates
- Rationale for the difficulty of choosing D3.js (requiring crossbuilding and a custom implementation of graphs) over the ease of Zeppelin is due to Zeppelin does not enable the features required for this visualization

# P2 Quaternary Objective
- ## Realtime Layer
- Spark Streaming
- Stream in timestamp data as if it was in Realtime
- Cut and sort .csv in order of time, then stream in through a StreamContext into a DStream
- Use rest of logic with RDD transformations and analytics