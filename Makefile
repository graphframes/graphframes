all: 1.4.1 1.5.2 1.6.1

clean:
	rm -rf target/graphframes_*.zip

1.4.1 1.5.2 1.6.1:
	build/sbt -Dspark.version=$@ spDist
