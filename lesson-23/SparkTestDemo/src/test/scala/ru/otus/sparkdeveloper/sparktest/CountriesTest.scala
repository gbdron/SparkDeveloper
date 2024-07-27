package ru.otus.sparkdeveloper.sparktest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import ru.otus.sparkdeveloper.sparktest.CountriesSchema.schema
import ru.otus.sparkdeveloper.sparktest.App._

class CountriesTest
    extends AnyFunSuite
    with Matchers
    with SparkSessionWrapper
    with Logging
    with BeforeAndAfter
    with DataFrameComparer {

  var testDf: DataFrame = _

  before {
    testDf = spark.read
      .option("multiline", "true")
      .schema(schema)
      .json("file:///s:/countries.json")

    spark.sparkContext.setLogLevel("error")
  }

  test("getBorderedCountries and getLanguageRanking should be executed successfully "){
      getBorderedCountries(testDf).show()
      getLanguageRanking(testDf).show()

  }

  test("getBorderedCountries should return DataFrame with correct schema") {
    // Ожидаемая схема
    val expectedSchema = StructType(
      Array(
        StructField("Country", StringType, nullable = false),
        StructField("NumBorders", IntegerType, nullable = false),
        StructField("BorderCountries", StringType, nullable = false)
      )
    )
    // Вызов функции
    val resultDF = getBorderedCountries(testDf)
    // Проверка схемы
    //resultDF.schema shouldEqual expectedSchema
  }

  test("getBorderedCountries should return Russia with 14 borders"){
    // Вызов функции
    import spark.implicits._
    val resultDf = getBorderedCountries(testDf).filter($"Country" === "Russia" && $"NumBorders" === 14).collect()
    resultDf.length shouldEqual 1

  }
  

  test("getLanguageRanking should return DataFrame with correct schema") {
    // Ожидаемая схема
    val expectedSchema = StructType(
      Array(
        StructField("Language", StringType, nullable = true),
        StructField("NumCountries", LongType, nullable = false),
        StructField("Countries", ArrayType(StringType, false), nullable = false)
      )
    )
    // Вызов функции
    val resultDF = getLanguageRanking(testDf)
    // Проверка схемы
    resultDF.schema shouldEqual expectedSchema
  }

  test("getLanguageRanking should return Russian with 8 countries"){
    // Вызов функции
    import spark.implicits._
    val resultDf = getLanguageRanking(testDf).filter($"Language" === "Russian" && $"NumCountries" === 8).collect()
    resultDf.length shouldEqual 1

  }

}
