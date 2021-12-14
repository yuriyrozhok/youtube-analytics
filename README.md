# youtube-trends
Reads and stores youtube stats, prepares simple analysis of trending videos

## unpack the input data
`brew install p7zip`

`7za x -oinput input/USvideos.7z`

## build the Spark app
`mvn package` 

## run the Spark app
Assumes that Spark is installed locally:

`./src/main/shell/runSparkApp.sh` 