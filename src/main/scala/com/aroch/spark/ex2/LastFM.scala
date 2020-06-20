package com.aroch.spark.ex2

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LastFM {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("Interview").master("local[*]").getOrCreate()

    // userid \t timestamp \t musicbrainz-artist-id \t artist-name \t musicbrainz-track-id \t track-name
    val userPlaysSchema = new StructType().
      add("userId", StringType).
      add("timestamp", TimestampType).
      add("artistId", StringType).
      add("artistName", StringType).
      add("trackId", StringType).
      add("trackName", StringType)

    var userPlays = spark.read.option("delimiter", "\t").schema(userPlaysSchema).csv("lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")

    val distinctSongsByUserId = userPlays.groupBy("userId")
      .agg(
        countDistinct("trackId")
      )

    distinctSongsByUserId.write.csv("output/distinctSongsCount")

    val topTracks = userPlays
      .where(col("trackId").isNotNull)
      .groupBy("trackId")
      .agg(
        max("artistName").as("artistName"),
        max("trackName").as("trackName"),
        count("artistName").as("timesPlayed")
      )
      .orderBy(desc("timesPlayed"))
      .limit(100)
      .drop("trackId")

    topTracks.write.csv("output/topTracks")

    val inactivityGapMinutes = 20

    val windowSpec = Window.partitionBy("userId").orderBy(asc("timestamp"))

    userPlays = userPlays
      .where(col("trackId").isNotNull)
      .withColumn("diff", col("timestamp").cast(LongType) - lag(col("timestamp").cast(LongType), 1, 0).over(windowSpec))
      .withColumn("hasGap", col("diff").gt(lit(inactivityGapMinutes * 60)).cast(IntegerType))
      .withColumn("sessionId", concat(col("userId"), sum("hasGap").over(windowSpec)))

    val longestSessions = userPlays
      .groupBy("sessionId")
      .agg(
        min("timestamp").as("sessionStart"),
        max("timestamp").as("sessionEnd")
      )
      .withColumn("sessionDuration", col("sessionEnd").cast(LongType) - col("sessionStart").cast(LongType))
      .orderBy(desc("sessionDuration"))
      .limit(10)

    val longestSessionsTracks = longestSessions
      .join(userPlays, "sessionId")
      .select("userId", "sessionDuration", "sessionStart", "sessionEnd", "trackName")

    longestSessionsTracks.write.csv("output/longestSessions")

    // id     gender  age     country registered
    val userProfileSchema = new StructType().
      add("userid", StringType).
      add("gender", StringType).
      add("age", IntegerType).
      add("country", StringType).
      add("registered", DateType)

    val userProfile = spark.read.option("dateFormat", "MMM dd, yyyy").option("header", value = true).option("delimiter", "\t").schema(userProfileSchema).csv("lastfm-dataset-1K/userid-profile.tsv")

    userProfile.show(false)

    spark.stop()
  }

}
