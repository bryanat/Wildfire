# Wildfire (P2 Spark)

### Agile/Scrum
- https://trello.com/b/YjETnJ2h/p2-scrum-agile

![](dataset-online/wildfire-ppt-images/wf2.jpg)

# Project Summary
Big Data - Trends and Analysis of Wildfire Data from Kaggle via Spark

**Description**: Spark application that demonstrates multiple stages of the Big Data pipeline using two large, real-life datasets and batch processing.

**Roles and Responsibilities**:
- Led a team of 4 members to Ingest > Store > Process > Query > Analyze > Viz batch data of over 2.6GB (1.9 Million rows) of Wildfire data joined onto a table with 3.5GB of Weather data called from an API.
- Joined the two datasets together into a single table using time as a foreign key.
- Processed the data via Spark and constructed Dataframes vis Spark SQL queries.
- Used Hive and HQL queries to analyze the data for trends between historical timestamped wildfire data and geographic weather data.
- Used Spark MLlib for further analysis and exploration with elementary ML models.
- Visualizations of query results with Zeppelin and Spark MLlib model with D3.js
- Optimization techniques such as broadcast joins, partitions, csv (row-oriented) to parquet files (columnar), and filter transformations before other transformations.
- Coding with Scala in VSCode utilizing Spark framework at the core.
- Implemented lean agile planning, scrum sprints, and constantly merged team members code into the master branch.
- Managed team members objectives and assisted them through roadblocks, reviewed code, resolved merge conflicts.
- Code reviews performed daily.

**Key Technologies**: Spark, Spark SQL, Spark Streaming, Hive, HDFS, Zeppelin

### Project Members (Group A)
- Bryan Boyett
- Yueqi Peng
- Abby Harrah
- Brandon Conover

![](docs/images/spark-tech.jpg)

# Tech
- Spark [RDD, Dataset, Dataframe]
- Spark Streaming [DStream]
- Spark MLlib [Vector]
- Spark SQL [Dataframe]
- D3.js [Scala.js crossbuild]

# Stage 0: Dataset
- Wildfire (1.88 Million US Wildfires @Kaggle)
  - https://www.kaggle.com/rtatman/188-million-us-wildfires
- Weather API 
- Wildfire<>Time<>>Weather 
  - Connect Wildfire dataset with Weather API using unix Timestamp to connect the two datasets (like a foreign key)
  - Timestamp provides granularity down to seconds but can simplify to daily granularity, as no real value gained from minutes overs daily 
  - An alternative third dataset option, US Wildfires, was chosen as it took the best features from the two winning datasets we narrowed it down to from all the datasets we looked through, along with an additional unique approach: by using the time field in US Wildfires as a sort of foreign key and primary key we could connect any additional dataset that had a time field. We are connecting additional Weather data to use as features for the Wildfire targets.

# Stage 1: Query Layer (Summarize)
- Create a Spark Application that process data(choose your data).
- Size of data should be 2k and above,and a minimum 3 tables.
- Your project  should involve useful analysis of data(Every concept of spark like rdd, dataframes, sql, dataset and optimization methods  and  persistence should be included). The Expected output is different trends that you have observed as part of data collectivley and how you can use these trends to make some useful decisions.
-Should have admin and normal user access with password set in database along with Visualization  for out put 
-Let the P2, have presentation with screen shots and practical demo.

# Stage 2: Analytics Layer
- MLlib for Analytical Trends in the Wildfire+Weather data
- notable weather data: Humidity, Temperature, Precipitation/Rainfall, Pressure,
- geographical distribution of fires
  - example: Class G fires (5,000+ acre fires) are dominantly in California
  - what is it about California that predisposes it to Class G? Drought (Precipitation/Rainfall), Humidity, Temperature, ???

# Stage 3: Visualization Layer
- D3.js for US Map Geographic Coordinates 
- Timestamp column for visualization over time as if Realtime
- Lat & Lon columns for Geographic Coordinates
- Rationale for the difficulty of choosing D3.js (requiring crossbuilding and a custom implementation of graphs) over the ease of Zeppelin is due to Zeppelin does not enable the features required for this visualization

# Stage 4: Realtime Layer (Streaming)
- Spark Streaming
- Stream in timestamp data as if it was in Realtime
- Cut and sort .csv in order of time, then stream in through a StreamContext into a DStream
- Use rest of logic with RDD transformations and analytics



![](dataset-online/wildfire-ppt-images/wf1.jpg)
![](dataset-online/wildfire-ppt-images/wf2.jpg)
![](dataset-online/wildfire-ppt-images/wf3.jpg)
![](dataset-online/wildfire-ppt-images/wf4.jpg)
![](dataset-online/wildfire-ppt-images/wf5.jpg)
![](dataset-online/wildfire-ppt-images/wf6.jpg)
![](dataset-online/wildfire-ppt-images/wf7.jpg)
![](dataset-online/wildfire-ppt-images/wf8.jpg)
![](dataset-online/wildfire-ppt-images/wf9.jpg)
![](dataset-online/wildfire-ppt-images/wf10.jpg)
![](dataset-online/wildfire-ppt-images/wf11.jpg)
![](dataset-online/wildfire-ppt-images/wf12.jpg)
![](dataset-online/wildfire-ppt-images/wf13.jpg)
![](dataset-online/wildfire-ppt-images/wf14.jpg)
![](dataset-online/wildfire-ppt-images/wf15.jpg)
![](dataset-online/wildfire-ppt-images/wf16.jpg)
![](dataset-online/wildfire-ppt-images/wf17.jpg)
![](dataset-online/wildfire-ppt-images/wf18.jpg)
![](dataset-online/wildfire-ppt-images/wf19.jpg)
![](dataset-online/wildfire-ppt-images/wf20.jpg)
![](dataset-online/wildfire-ppt-images/wf21.jpg)
![](dataset-online/wildfire-ppt-images/wf22.jpg)
