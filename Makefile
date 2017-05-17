all: 2.1.1s2.10 1.6.3 2.0.2 2.1.1

clean:
	rm -rf target/graphframes_*.zip

1.6.3 2.0.2 2.1.1:
	build/sbt -Dspark.version=$@ spDist

2.1.1s2.10:
	build/sbt -Dspark.version=2.1.1 -Dscala.version=2.10.6 spDist assembly test
