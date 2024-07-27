package ru.otus.sparkdeveloper.sparktest

import org.apache.logging.log4j.scala.Logging
import ru.otus.sparkdeveloper.sparktest.CountriesSchema.schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._



object App extends Logging with SparkSessionWrapper {
  import spark.implicits._
  def getBorderedCountries(df: DataFrame): DataFrame = {
    // Вычисляем количество граничащих стран
    val countriesWithBordersCount = df
      .withColumn("NumBorders", size(col("borders")))
      .filter(col("NumBorders") >= 5)

    // Преобразуем список граничащих стран в строку
    val countriesWithBorderCountries = countriesWithBordersCount
      .withColumn("BorderCountries", concat_ws(",", col("borders")))
      .select(
        col("name.common").alias("Country"),
        col("NumBorders"),
        col("BorderCountries")
      )
      .orderBy(desc("NumBorders"))

    countriesWithBorderCountries
  }
  
  // Функция для вычисления рейтинга языков
  def getLanguageRanking(df: DataFrame): DataFrame = {
    
    // Развернуть карту языков и создать DataFrame с двумя столбцами: language и country
    val explodedDF = df
      .selectExpr(
        "explode(map_values(languages)) as Language",
        "name.common as country"
      )
    
    // Группировка по языкам и подсчет уникальных стран
    explodedDF
      .groupBy($"Language")
      .agg(
        countDistinct($"country").as("NumCountries"),
        collect_list($"country").as("Countries")
      )
      .orderBy(desc("NumCountries"))
  }

  def main(args: Array[String]): Unit = {

    println("hello")
    spark.sparkContext.setLogLevel("error")

    val df = spark.read
      .option("multiline", "true")
      .schema(schema)
      .json("file:///s:/countries.json")

    

    val borderedDf = getBorderedCountries(df)
    borderedDf.show()

    val langDf = getLanguageRanking(df)
    langDf.show
    
    

  }

}
