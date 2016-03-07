echo "Downloading Spark if necessary"
echo "Spark version = $SPARK_VERSION"
echo "Spark build = $SPARK_BUILD"
echo "Spark build URL = $SPARK_BUILD_URL"
mkdir -p $HOME/.cache/spark-versions
filename="$HOME/.cache/spark-versions/spark-1.6.0-bin-hadoop2.6.tgz"
if ! [ -f $filename ]; then
	echo "Downloading file..."
	echo `which curl`
	curl http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz > $filename
	echo "Content of directory:"
	ls -la $HOME/.cache/spark-versions/*
	tar xvf $filename --directory $HOME/.cache/spark-versions
fi
