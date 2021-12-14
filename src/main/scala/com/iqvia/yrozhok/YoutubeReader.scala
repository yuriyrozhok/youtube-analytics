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

    /* this demonstrates that trending_date format is yy.dd.MM
     * scala> v1.select("trending_date").distinct.orderBy(lit(1).desc).show(10)
        +-------------+
        |trending_date|
        +-------------+
        |     18.31.05|
     */
    //videos.groupBy("trending_date").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").desc).show(10)
    //196 - 200 videos daily
    //videos.groupBy("category_id").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(50)
    //57 - 9964 videos per category
    val videos = spark.read
    //.json("./sample/video-top1k.json")
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
    //videos.groupBy("trending_date").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(10)
    //videos.groupBy("category_id").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(50)
    //videos.createOrReplaceTempView("USvideos")

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
    //trending.groupBy("trending_date").agg(count("video_id").as("vidnum")).orderBy(col("vidnum").asc).show(10)
    //trending.orderBy("trending_date", "category_id", "title").show
    //spark.sql(queryText).show(returnRows)
    //.option("maxRecordsPerFile", 1000)

    videos
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("trending_date")
      .parquet("./output/USvideos.source")

    trending
      .repartition(1)
      .write
      .parquet("./output/USvideos.trending")
  }
}
