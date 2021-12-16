# place the input file into HDFS:
# hadoop fs -mkdir -p /development/de9/data/arz/temp/yrozhok
# hadoop fs -put usv.json /development/de9/data/arz/temp/yrozhok
# hadoop fs -ls /development/de9/data/arz/temp/yrozhok
# Found 1 items
# -rw-r--r--+  3 de9darz de9devlarz   77696464 2021-12-15 14:29 /development/de9/data/arz/temp/yrozhok/usv.json

spark2-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "yrozhok-usv-reader" \
  --conf spark.yarn.queue=root.de9arz \
  --principal de9darz \
  --keytab ~/keytab/de9darz.keytab \
  --driver-memory 2g \
  --driver-cores 4 \
  --executor-memory 4g \
  --executor-cores 5 \
  --conf spark.sql.shuffle.partitions=40 \
  --conf spark.files.maxPartitionBytes=134217728 \
  --conf spark.default.parallelism=40 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=8 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.shuffle.service.enabled=true \
  --conf hive.exec.dynamic.partition=true \
  --conf hive.exec.dynamic.partition.mode=nonstrict \
  --class com.iqvia.yrozhok.YoutubeReader \
  ./youtube-1.0-SNAPSHOT.jar cluster
