all: 2.0.2s2.10 1.6.3 2.0.2

clean:
	rm -rf target/graphframes_*.zip

1.6.3 2.0.2:
	build/sbt -Dspark.version=$@ spDist

2.0.2s2.10:
	build/sbt -Dspark.version=2.0.2 -Dscala.version=2.10.4 spDist assembly test
