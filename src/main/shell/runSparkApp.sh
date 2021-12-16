#!/bin/bash
spark-submit --master local[2] \
--class com.iqvia.yrozhok.YoutubeReader \
./target/youtube-1.0-SNAPSHOT.jar local