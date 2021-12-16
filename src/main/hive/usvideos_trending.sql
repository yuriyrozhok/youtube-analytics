--US videos source data
CREATE TABLE IF NOT EXISTS devl_de9_arz_batch.yrozhok_usv_trd(
    video_id STRING,
    category_id INT,
    title STRING,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT
) PARTITIONED BY (trending_date INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
