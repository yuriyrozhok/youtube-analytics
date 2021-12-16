# youtube-trends
Reads and stores youtube stats, prepares simple analysis of trending videos

## unpack the input data
`brew install p7zip`

`7za x -oinput input/USvideos.7z`

## build the Spark app
`mvn package` 

## parameters
this app accepts one argument from the command line, which drives the execution mode and takes values:
- local - in this mode app reads from the local JSON file and produces local Parquet files 
- cluster - in this mode app reads from HDFS JSON file and produces records in Hive table (see included scripts) 
 
## run the Spark app on the local machine
Assumes that Spark is installed locally, run from the project root:\
`./src/main/shell/runSparkApp.sh` \
this will produce new folders/files in ./output folder:
- USvideos.source - copy of the data from the source JSON
- USvideos.trending - trending videos

## run the Spark app in cluster
Assumes that you run it from the edge node, app JAR and keytab are also available there:\
`./runSparkAppCluster.sh` \
this will produce new records in Hive tables:
- yrozhok_usv_src - copy of the data from the source JSON
- yrozhok_usv_trd - trending videos

## screenshots
see some screenshots with results in /img folder