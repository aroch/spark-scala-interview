package com.aroch.spark.ex1

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ProcessingTimeToEventTime {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("Interview").master("local[*]").getOrCreate()

  }

}
