VERSION=0.1.0
SCALA_VERSION=2.10
PACKAGE_BASE_NAME=dist/graphframes_$(SCALA_VERSION)-$(VERSION)-spark

$(PACKAGE_BASE_NAME)1.4.jar:
	$(eval USE_SPARK_VERSION := 1.4.1)
	$(eval SPARK_DISPLAY := 1.4)
	build/sbt -Dspark.version=$(USE_SPARK_VERSION) "clean" "spPackage"
	mv target/scala-$(SCALA_VERSION)/graphframes_$(SCALA_VERSION)-$(VERSION)-SNAPSHOT.jar $(PACKAGE_BASE_NAME)$(SPARK_DISPLAY).jar

$(PACKAGE_BASE_NAME)1.5.jar:
	$(eval USE_SPARK_VERSION := 1.5.1)
	$(eval SPARK_DISPLAY := 1.5)
	build/sbt -Dspark.version=$(USE_SPARK_VERSION) "clean" "spPackage"
	mv target/scala-$(SCALA_VERSION)/graphframes_$(SCALA_VERSION)-$(VERSION)-SNAPSHOT.jar $(PACKAGE_BASE_NAME)$(SPARK_DISPLAY).jar

$(PACKAGE_BASE_NAME)1.6.jar:
	$(eval USE_SPARK_VERSION := 1.6.0)
	$(eval SPARK_DISPLAY := 1.6)
	build/sbt -Dspark.version=$(USE_SPARK_VERSION) "clean" "spPackage"
	mv target/scala-$(SCALA_VERSION)/graphframes_$(SCALA_VERSION)-$(VERSION)-SNAPSHOT.jar $(PACKAGE_BASE_NAME)$(SPARK_DISPLAY).jar

clean:
	rm -rf dist/graphframes_*jar

all: $(PACKAGE_BASE_NAME)1.4.jar $(PACKAGE_BASE_NAME)1.5.jar $(PACKAGE_BASE_NAME)1.6.jar