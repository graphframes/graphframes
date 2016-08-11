#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# assumes run from python/ directory
if [ -z "$SPARK_HOME" ]; then
    echo 'You need to set $SPARK_HOME to run these tests.' >&2
    exit 1
fi

LIBS=""
for lib in "$SPARK_HOME/python/lib"/*zip ; do
  LIBS=$LIBS:$lib
done

# The current directory of the script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

a=( ${SCALA_VERSION//./ } )
scala_version_major_minor="${a[0]}.${a[1]}"
echo "List of assembly jars found, the last one will be used:"
assembly_path="$DIR/../target/scala-$scala_version_major_minor"
echo `ls $assembly_path/graphframes-assembly*.jar`
JAR_PATH=""
for assembly in $assembly_path/graphframes-assembly*.jar ; do
  JAR_PATH=$assembly
done

export PYSPARK_SUBMIT_ARGS="--jars $JAR_PATH pyspark-shell "

export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$LIBS:.

export PYTHONPATH=$PYTHONPATH:graphframes

# Return on any failure
set -e

# Run test suites

# Horrible hack for spark 1.4: we manually remove some log lines to stay below the 4MB log limit on Travis.
# To remove when we ditch spark 1.4.
nosetests -v --all-modules -w $DIR  2>&1 | grep -vE "INFO (ShuffleBlockFetcherIterator|MapOutputTrackerMaster|TaskSetManager|Executor|MemoryStore|CacheManager|BlockManager|DAGScheduler|PythonRDD|TaskSchedulerImpl|ZippedPartitionsRDD2)"


# Run doc tests

cd "$DIR"

exec python -u ./graphframes/graphframe.py "$@"
