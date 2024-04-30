// Databricks notebook source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// COMMAND ----------

val conf = new SparkConf().setAppName("Word Count")

// COMMAND ----------

val sc = SparkContext.getOrCreate(conf)

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/sample13.txt")

// COMMAND ----------

val data2 = data.map(line => line + " Hello")
data2.collect()

// COMMAND ----------

val splitData = data2.flatMap(line => line.split(" "))
splitData.collect()

// COMMAND ----------

val mapdata = splitData.map(word=> (word,1))
mapdata.collect()

// COMMAND ----------

val reduceData = mapdata.reduceByKey(_+_)

// COMMAND ----------

reduceData.collect()
