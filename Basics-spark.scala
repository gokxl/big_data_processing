// Databricks notebook source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// COMMAND ----------

val conf = new SparkConf().setAppName("Scala Spark")

// COMMAND ----------

val sc = SparkContext.getOrCreate(conf)

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/sample13.txt")

// COMMAND ----------

data.collect()

// COMMAND ----------

val data2 = data.map(line => line + " Hello")

// COMMAND ----------

data2.collect()

// COMMAND ----------

val data3 = data2.flatMap(line1 => line1.split(" "))
data3.collect()

// COMMAND ----------


