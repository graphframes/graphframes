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

from .graphframe import GraphFrame

__all__ = ['Graphs']

class Graphs(object):
    """
    Example GraphFrames
    """

    def __init__(self, sqlContext):
        """
        :param sqlContext: SQLContext
        """
        self._sql = sqlContext
        self._sc = sqlContext._sc

    def friends(self):
        """
        A GraphFrame of friends in a (fake) social network.
        """
        sqlContext = self._sql
        # Vertex DataFrame
        v = sqlContext.createDataFrame([
            ("a", "Alice", 34),
            ("b", "Bob", 36),
            ("c", "Charlie", 30),
            ("d", "David", 29),
            ("e", "Esther", 32),
            ("f", "Fanny", 36)
        ], ["id", "name", "age"])
        # Edge DataFrame
        e = sqlContext.createDataFrame([
            ("a", "b", "friend"),
            ("b", "c", "follow"),
            ("c", "b", "follow"),
            ("f", "c", "follow"),
            ("e", "f", "follow"),
            ("e", "d", "friend"),
            ("d", "a", "friend")
        ], ["src", "dst", "relationship"])
        # Create a GraphFrame
        return GraphFrame(v, e)
