# Predictive Maintenance Using Sensor Analytics on Cloudera
Created by Samir Gupta (sgupta@cloudera.com)
Original Demo Repo: https://github.com/tomatoTomahto/CDH-Sensor-Analytics

For information on use case and technical architecture, see PDF presentation in ```slides``` directory. 

## Pre-Requisites
1. CDH 5.10, including the following services:
 * Impala
 * Kafka (optional - for real-time ingest part of demo)
 * KUDU (optional - if not using sample data provided)
 * Spark 2.0
 * Solr (optional)
 * HUE
2. Cloudera Data Science Workbench
3. Anaconda Parcel for Cloudera
4. Streamsets Data Collector (optional)

## Generate Data (optional - if re-generating sample data or storing data in Kudu)
1. Edit config.ini with the desired data generator parameters (# sensors etc.) and hadoop settings (kafka and kudu servers)
2. Create tables to store sensor data in Kudu and generate static lookup information:
```python datagen/historian.py config.ini static kudu```
3. Generate historic data and store in Kudu
```python datagen/historian.py config.ini historic kudu```
4. Open Kudu web UI and navigate to the tables that were created, extract Impala DDL statements and run them in HUE

## Data Science
1. In the Cloudera Data Science Workbench, create a new project using this git repository
2. Add spark-defaults.conf to the project environment settings
3. Create a workbench with at least 4 cores and 8GB RAM using PySpark
4. Run the cdsw/SensorAnalytics_kudu.py script

## Real-time Ingest (optional)
1. Start generating real-time data using the data generator
```python datagen/historian.py config.ini realtime```
2. In the Cloudera Data Science Workbench, run the cdsw/SensorAnalytics_stream.py script
3. (Optional) Create a pipeline in Streamsets to read from Kafka, transform data, and write to Kudu or HDFS 

## Analyze
1. Solr (not included in git repo)
 * Create a collection for the measurements data
 * Create a Hue dashboard based on the measurements data
 * Add a destination in Streamsets to send measurement data to Solr collection
2. Impala - open HUE and run queries on the raw_measurements and measurements tables
