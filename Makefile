all: 1.4.1 1.5.2 1.6.2 2.0.0

clean:
	rm -rf target/graphframes_*.zip

1.4.1 1.5.2 1.6.1 2.0.0:
	build/sbt -Dspark.version=$@ spDist
