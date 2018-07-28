all: 2.2.2s2.10 2.1.3 2.2.2 2.3.1

clean:
	rm -rf target/graphframes_*.zip

2.1.3 2.2.2 2.3.1:
	build/sbt -Dspark.version=$@ spDist

2.2.2s2.10:
	build/sbt -Dspark.version=2.2.2 -Dscala.version=2.10.6 spDist assembly test
