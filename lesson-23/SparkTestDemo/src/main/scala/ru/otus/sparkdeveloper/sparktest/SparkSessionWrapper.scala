package ru.otus.sparkdeveloper.sparktest

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkProdApp")
    .master("spark://192.168.1.50:7077")
    .getOrCreate
}
