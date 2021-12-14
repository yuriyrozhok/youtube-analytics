package com.iqvia.yrozhok

import org.apache.spark.sql.types.{
  BooleanType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

object Util {
  val columns = Seq(
    ("video_id", StringType),
    ("trending_date", StringType),
    ("title", StringType),
    ("channel_title", StringType),
    ("category_id", StringType),
    ("publish_time", TimestampType),
    ("tags", StringType),
    ("views", LongType),
    ("likes", LongType),
    ("dislikes", LongType),
    ("comment_count", LongType),
    ("thumbnail_link", StringType),
    ("comments_disabled", BooleanType),
    ("ratings_disabled", BooleanType),
    ("video_error_or_removed", BooleanType),
    ("description", StringType)
  )
  val schema = StructType(columns.map(x => StructField(x._1, x._2, true)))
  //schema is not used because input contains all columns as strings, even numbers

  val infSchema = StructType(
    Seq(
      StructField("category_id", IntegerType, true),
      StructField("channel_title", StringType, true),
      StructField("comment_count", StringType, true),
      StructField("comments_disabled", StringType, true),
      StructField("description", StringType, true),
      StructField("dislikes", StringType, true),
      StructField("likes", StringType, true),
      StructField("publish_time", TimestampType, true),
      StructField("ratings_disabled", StringType, true),
      StructField("tags", StringType, true),
      StructField("thumbnail_link", StringType, true),
      StructField("title", StringType, true),
      StructField("trending_date", StringType, true),
      StructField("video_error_or_removed", StringType, true),
      StructField("video_id", StringType, true),
      StructField("views", StringType, true)
    )
  )

  //import spark.implicits._
  /*
  val videos = spark.read
    .option("multiline", "true")
    .schema(schema)
    .json("./sample/video-top1.json")
 */

}
