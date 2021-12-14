package com.iqvia.yrozhok

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object YoutubeReader {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("YoutubeReader")
      .enableHiveSupport()
      .getOrCreate()

    // ingest the source JSON with inferred schema and cast the data types for some columns
    val videos = spark.read
      .json("./input/USvideos.json")
      .select(
        "video_id",
        "trending_date",
        "category_id",
        "title",
        "views",
        "likes",
        "dislikes"
      )
      .withColumn("trending_date", to_date(col("trending_date"), "yy.dd.MM"))
      .withColumn("category_id", col("category_id").cast(IntegerType))
      .withColumn("views", col("views").cast(LongType))
      .withColumn("likes", col("likes").cast(LongType))
      .withColumn("dislikes", col("dislikes").cast(LongType))
    /* this demonstrates that trending_date format is yy.dd.MM
     * scala> videos.select("trending_date").distinct.orderBy(lit(1).desc).show(10)
        +-------------+
        |trending_date|
        +-------------+
        |     18.31.05|
     *
     * Some stats on the input data:
        videos.groupBy("trending_date").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").desc).show(10)
        -> 196 - 200 videos daily
        videos.groupBy("category_id").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(50)
        -> 57 - 9964 videos per category
        videos.select("category_id").distinct.count
        -> 16 categories included
        videos.select("trending_date").distinct.count
        -> 205 dates included
     */

    //videos.groupBy("trending_date").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(10)
    //videos.groupBy("category_id").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(50)

    // keep only the most viewed videos per date and category
    val trending = videos
      .withColumn(
        "view_rank",
        rank.over(
          Window
            .partitionBy(col("trending_date"), col("category_id"))
            .orderBy(col("views").desc)
        )
      )
      .filter(col("view_rank") === 1)
      .drop("view_rank")
    /*
    the same can be done with SQL:
    videos.createOrReplaceTempView("USvideos")
    val trendSql = spark.sql(
      """
        |SELECT video_id, trending_date, category_id, title, views, likes, dislikes
        |FROM (
        |  SELECT *, RANK() OVER (PARTITION BY trending_date, category_id ORDER BY views DESC) trank
        |  FROM USvideos
        |) usv
        |WHERE trank = 1
        |""".stripMargin
    )
     */

    /*
     * 77 MB of input JSON results in 1 MB of parquet
     * let's assume that daily input is 100 GB, then we should expect 1329 MB in output, which is too much for one file
     * best practice is to keep file size close to the HDFS block size
     * HDFS default setting is 128 MB per block, this means we should create roughly 10 files
     * in that case we should repartition the dataframe into 10 partitions
     * and make sure they can be written in parallel (by setting executor cores and number of executors)
     * depending on the real data, we may distribute the file based on category
     * for better size control, we can limit the number of records in the file by .option("maxRecordsPerFile", 100...)
     * */
    videos
      .repartition(col("category_id"))
      .write
      .partitionBy("trending_date")
      .mode(SaveMode.Append)
      .parquet("./output/USvideos.source")

    /*
     * trending videos results in quite small output
     * for 77 MB of input JSON we get 88 KB of parquet
     * it's because we extract roughly one video per category per day
     * for the given JSON it produces 2870 records only (205 dates and 16 categories)
     * even if we assume in real dataset 10 years of data and 200 categories,
     * this will produce 10*365*200 = 730K records, which results roughly in 20 MB
     * doesn't make sense to partition with the given assumptions
     * though, if needed, it can be partitioned by trending_date
     * */
    trending
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("./output/USvideos.trending")
  }
}
