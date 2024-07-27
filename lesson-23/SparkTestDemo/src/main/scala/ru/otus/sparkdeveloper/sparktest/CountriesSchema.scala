package ru.otus.sparkdeveloper.sparktest

import org.apache.spark.sql.types._

object CountriesSchema{
val schema = StructType(Array(
      StructField("name", StructType(Array(
        StructField("common", StringType, nullable = true),
        StructField("official", StringType, nullable = true),
        StructField("native", StructType(Array(
          StructField("nld", StructType(Array(
            StructField("official", StringType, nullable = true),
            StructField("common", StringType, nullable = true)
          )), nullable = true),
          StructField("pap", StructType(Array(
            StructField("official", StringType, nullable = true),
            StructField("common", StringType, nullable = true)
          )), nullable = true)
        )), nullable = true)
      )), nullable = true),
      StructField("tld", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("cca2", StringType, nullable = true),
      StructField("ccn3", StringType, nullable = true),
      StructField("cca3", StringType, nullable = true),
      StructField("cioc", StringType, nullable = true),
      StructField("independent", BooleanType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("unMember", BooleanType, nullable = true),
      StructField("currencies", StructType(Array(
        StructField("AWG", StructType(Array(
          StructField("name", StringType, nullable = true),
          StructField("symbol", StringType, nullable = true)
        )), nullable = true)
      )), nullable = true),
      StructField("idd", StructType(Array(
        StructField("root", StringType, nullable = true),
        StructField("suffixes", ArrayType(StringType, containsNull = true), nullable = true)
      )), nullable = true),
      StructField("capital", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("altSpellings", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("region", StringType, nullable = true),
      StructField("subregion", StringType, nullable = true),
      StructField("languages", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
      StructField("translations", MapType(StringType, StructType(Array(
        StructField("official", StringType, nullable = true),
        StructField("common", StringType, nullable = true)
      )), valueContainsNull = true), nullable = true),
      StructField("latlng", ArrayType(DoubleType, containsNull = true), nullable = true),
      StructField("landlocked", BooleanType, nullable = true),
      StructField("borders", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("area", IntegerType, nullable = true),
      StructField("flag", StringType, nullable = true),
      StructField("demonyms", MapType(StringType, StructType(Array(
        StructField("f", StringType, nullable = true),
        StructField("m", StringType, nullable = true)
      )), valueContainsNull = true), nullable = true)
    ))
}