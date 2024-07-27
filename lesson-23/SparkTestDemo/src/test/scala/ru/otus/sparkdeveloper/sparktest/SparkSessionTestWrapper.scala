package ru.otus.sparkdeveloper.sparktest

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkTestApp")
    .master("spark://192.168.1.50:7077")
    .getOrCreate

}
