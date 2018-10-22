package com.aroch.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Main {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("Interview").master("local[*]").getOrCreate()

    import spark.implicits._

  }

}
