package com.aroch.spark.ex1

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ProcessingTimeToEventTime {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("ex1").master("local[*]").getOrCreate()

    val schema1 = new StructType()
      .add("time", TimestampType)
      .add("type", StringType)
      .add("uid", StringType)
      .add("country", StringType)

    val schema2 = new StructType()
      .add("time", LongType)
      .add("type", StringType)
      .add("uid", StringType)
      .add("country", StringType)

    val source1 = spark.read
      .option("header", "true")
      .option("timestampFormat", "dd MMM yyyy HH:mm:ss z")
      .schema(schema1)
      .csv("src/main/resources/source_1/*/*/*/*")

    val source2 = spark.read
      .option("header", "true")
      .schema(schema2)
      .csv("src/main/resources/source_2/*/*/*/*")

    val union = source2.select(
      col("time").cast(TimestampType),
      col("type"),
      col("uid"),
      col("country")
    )
      .union(source1)
      .withColumn("year", year(col("time")))
      .withColumn("month", month(col("time")))
      .withColumn("day", dayofmonth(col("time")))
      .orderBy(desc("time"))
      .coalesce(1)

    union
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .csv("output")

    spark.sparkContext.stop()
  }

}
