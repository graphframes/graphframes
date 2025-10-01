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


from pyspark.sql import Column
from pyspark.sql import functions as F

from graphframes import graphframe


class AggregateMessages:
    """Collection of utilities usable with :meth:`graphframes.GraphFrame.aggregateMessages()`."""

    @staticmethod
    def src() -> Column:
        """Reference for source column, used for specifying messages."""
        return F.col(graphframe.SRC)

    @staticmethod
    def dst() -> Column:
        """Reference for destination column, used for specifying messages."""
        return F.col(graphframe.DST)

    @staticmethod
    def edge() -> Column:
        """Reference for edge column, used for specifying messages."""
        return F.col(graphframe.EDGE)

    @staticmethod
    def msg() -> Column:
        """Reference for message column, used for specifying aggregation function."""
        return F.col("MSG")
