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

from pyspark import SparkContext
from pyspark.sql import SQLContext

class DFGraph(object):

    """
    Represents a graph with vertices and edges stored as DataFrames.

    :param vertices:  [[DataFrame]] holding vertex information.
                      Must contain a column named "id" that stores unique
                      vertex IDs.
    :param edges:  [[DataFrame]] holding edge information.
                   Must contain two columns "src" and "dst" storing source
                   vertex IDs and destination vertex IDs of edges, respectively.

    >>> localVertices = [(1,"A"), (2,"B"), (3, "C")]
    >>> localEdges = [(1,2,"love"), (2,1,"hate"), (2,3,"follow")]
    >>> v = sqlContext.createDataFrame(localVertices, ["id", "name"])
    >>> e = sqlContext.createDataFrame(localEdges, ["src", "dst", "action"])
    >>> g = DFGraph(v, e)
    """
    def __init__(self, v, e):
        self._vertices = v
        self._edges = e
        self._sqlContext = v.sql_ctx
        self._sc = self._sqlContext._sc

        self.ID = "id"
        self.SRC = "src"
        self.DST = "dst"
        self._ATTR = "attr"

        assert self.ID in v.columns,\
            "Vertex ID column '%s' missing from vertex DataFrame, which has columns: %s" %\
            (self.ID, ",".join(v.columns))
        assert self.SRC in e.columns,\
            "Source vertex ID column '%s' missing from edge DataFrame, which has columns: %s" %\
            (self.SRC, ",".join(e.columns))
        assert self.DST in e.columns,\
            "Destination vertex ID column '%s' missing from edge DataFrame, which has columns: %s"%\
            (self.DST, ",".join(e.columns))

    @property
    def vertices(self):
        """
        [[DataFrame]] holding vertex information, with unique column "id"
        for vertex IDs.
        """
        return self._vertices

    @property
    def edges(self):
        """
        [[DataFrame]] holding edge information, with unique columns "src" and
        "dst" storing source vertex IDs and destination vertex IDs of edges,
        respectively.
        """
        return self._edges


def _test():
    import doctest
    import dfgraph
    globs = dfgraph.__dict__.copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    globs['sqlContext'] = SQLContext(globs['sc'])
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
