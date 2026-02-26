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

import hashlib
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from graphframes import GraphFrame
from graphframes.pg import EdgePropertyGroup, PropertyGraphFrame, VertexPropertyGroup


class PropertyGraphFrameTest(unittest.TestCase):
    """Test cases for PropertyGraphFrame."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.spark = SparkSession.builder.appName("PropertyGraphFrameTest").getOrCreate()

        # Create test data
        # People vertices
        people_data = cls.spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eve")],
            ["id", "name"],
        )
        cls.people_group = VertexPropertyGroup("people", people_data, "id")

        # Movies vertices
        movies_data = cls.spark.createDataFrame(
            [(1, "Matrix"), (2, "Inception"), (3, "Interstellar")],
            ["id", "title"],
        )
        cls.movies_group = VertexPropertyGroup("movies", movies_data, "id")

        # Likes edges (undirected, people to movies)
        likes_data = cls.spark.createDataFrame(
            [(1, 1), (1, 2), (2, 1), (3, 2), (4, 3), (5, 2)],
            ["src", "dst"],
        )

        # Add weight column to likes_data
        likes_data_with_weight = likes_data.withColumn("weight", lit(1.0))
        cls.likes_group = EdgePropertyGroup(
            "likes",
            likes_data_with_weight,
            cls.people_group,
            cls.movies_group,
            is_directed=False,
            src_column_name="src",
            dst_column_name="dst",
            weight_column_name="weight",
        )

        # Messages edges (directed, people to people)
        messages_data = cls.spark.createDataFrame(
            [(1, 2, 5.0), (2, 3, 8.0), (3, 4, 3.0), (4, 5, 6.0), (5, 1, 9.0)],
            ["src", "dst", "weight"],
        )
        cls.messages_group = EdgePropertyGroup(
            "messages",
            messages_data,
            cls.people_group,
            cls.people_group,
            is_directed=True,
            src_column_name="src",
            dst_column_name="dst",
            weight_column_name="weight",
        )

        # Create the property graph
        cls.people_movies_graph = PropertyGraphFrame(
            [cls.people_group, cls.movies_group],
            [cls.likes_group, cls.messages_group],
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures."""
        cls.spark.stop()

    @staticmethod
    def sha256_hash(id_val, group_name):
        """Helper to compute SHA256 hash like Scala does."""
        hash_val = hashlib.sha256(str(id_val).encode("utf-8")).hexdigest()
        return f"{group_name}{hash_val}"

    def test_property_graph_frame_constructor(self):
        """Test PropertyGraphFrame constructor."""
        self.assertEqual(len(self.people_movies_graph.vertex_property_groups), 2)
        self.assertEqual(len(self.people_movies_graph.edges_property_groups), 2)

    def test_vertex_property_group_creation(self):
        """Test VertexPropertyGroup creation."""
        self.assertEqual(self.people_group.name, "people")
        self.assertEqual(self.people_group.primary_key_column, "id")
        self.assertTrue(self.people_group.apply_mask_on_id)

    def test_edge_property_group_creation(self):
        """Test EdgePropertyGroup creation."""
        self.assertEqual(self.likes_group.name, "likes")
        self.assertEqual(self.likes_group.src_property_group.name, "people")
        self.assertEqual(self.likes_group.dst_property_group.name, "movies")
        self.assertFalse(self.likes_group.is_directed)

    def test_projection_by_movies(self):
        """Test projection by movies creates correct graph structure."""
        projected_graph = self.people_movies_graph.projection_by("people", "movies", "likes")

        self.assertEqual(len(projected_graph.vertex_property_groups), 1)
        self.assertEqual(projected_graph.vertex_property_groups[0].name, "people")

        self.assertEqual(len(projected_graph.edges_property_groups), 2)
        self.assertTrue(
            any(group.name == "messages" for group in projected_graph.edges_property_groups)
        )

        projected_edges_group = next(
            (
                group
                for group in projected_graph.edges_property_groups
                if group.name == "projected_likes"
            ),
            None,
        )
        self.assertIsNotNone(projected_edges_group)
        self.assertEqual(projected_edges_group.src_column_name, GraphFrame.SRC)
        self.assertEqual(projected_edges_group.dst_column_name, GraphFrame.DST)
        self.assertEqual(projected_edges_group.weight_column_name, GraphFrame.WEIGHT)
        self.assertFalse(projected_edges_group.is_directed)

        # Check projected edges
        projected_edges = projected_edges_group.data.collect()
        edge_pairs = {(row.src, row.dst) for row in projected_edges}

        # Expected edges between people who like the same movies
        expected_edges = {
            (1, 2),  # Alice and Bob both like Matrix
            (1, 3),  # Alice and Charlie both like Inception
            (1, 5),  # Alice and Eve both like Inception
            (3, 5),  # Charlie and Eve both like Inception
        }

        self.assertEqual(edge_pairs, expected_edges)

    def test_projection_with_custom_weight(self):
        """Test projection with custom weight function."""
        projected_graph = self.people_movies_graph.projection_by(
            "people", "movies", "likes", new_edge_weight=lambda w1, w2: w1 + w2
        )

        projected_edges_group = next(
            (
                group
                for group in projected_graph.edges_property_groups
                if group.name == "projected_likes"
            ),
            None,
        )
        self.assertIsNotNone(projected_edges_group)

        projected_edges = projected_edges_group.data.collect()
        edge_triples = {(row.src, row.dst, row.weight) for row in projected_edges}

        # Expected edges with sum of weights (1.0 + 1.0 = 2.0)
        expected_edges = {
            (1, 2, 2.0),
            (1, 3, 2.0),
            (1, 5, 2.0),
            (3, 5, 2.0),
        }

        self.assertEqual(edge_triples, expected_edges)

    def test_to_graph_frame_messages_only(self):
        """Test to_graph_frame with messages edges and people vertices only."""
        graph = self.people_movies_graph.to_graph_frame(
            vertex_property_groups=["people"],
            edge_property_groups=["messages"],
            edge_group_filters={"messages": lit(True)},
            vertex_group_filters={"people": lit(True)},
        )

        vertices = {row.id for row in graph.vertices.collect()}
        edges = {(row.src, row.dst, row.weight) for row in graph.edges.collect()}

        # Verify vertices (all people with hashed IDs)
        expected_vertices = {self.sha256_hash(i, "people") for i in range(1, 6)}
        self.assertEqual(vertices, expected_vertices)

        # Verify directed message edges with weights
        expected_edges = {
            (self.sha256_hash(1, "people"), self.sha256_hash(2, "people"), 5.0),
            (self.sha256_hash(2, "people"), self.sha256_hash(3, "people"), 8.0),
            (self.sha256_hash(3, "people"), self.sha256_hash(4, "people"), 3.0),
            (self.sha256_hash(4, "people"), self.sha256_hash(5, "people"), 6.0),
            (self.sha256_hash(5, "people"), self.sha256_hash(1, "people"), 9.0),
        }
        self.assertEqual(edges, expected_edges)

    def test_to_graph_frame_all_groups(self):
        """Test to_graph_frame with all groups."""
        graph = self.people_movies_graph.to_graph_frame(
            vertex_property_groups=["people", "movies"],
            edge_property_groups=["messages", "likes"],
            edge_group_filters={"messages": lit(True), "likes": lit(True)},
            vertex_group_filters={"people": lit(True), "movies": lit(True)},
        )

        vertices = graph.vertices.collect()
        edges = graph.edges.collect()

        # Verify all vertices are present (5 people + 3 movies)
        self.assertEqual(len(vertices), 8)

        # Verify vertex types are correctly preserved
        vertex_ids = {row.id for row in vertices}
        self.assertIn(self.sha256_hash(1, "movies"), vertex_ids)
        self.assertIn(self.sha256_hash(1, "people"), vertex_ids)

        # Verify edge counts
        message_edges = [e for e in edges if e.weight != 1.0]
        like_edges = [e for e in edges if e.weight == 1.0]

        self.assertEqual(len(message_edges), 5)  # Directed messages
        self.assertEqual(len(like_edges), 12)  # 6 undirected edges * 2

    def test_to_graph_frame_unmasked_ids(self):
        """Test to_graph_frame preserves original IDs when masking disabled."""
        # Create movies group with masking disabled
        movies_data = self.spark.createDataFrame(
            [(1, "Matrix"), (2, "Inception"), (3, "Interstellar")],
            ["id", "title"],
        )
        unmasked_movies_group = VertexPropertyGroup(
            "movies", movies_data, "id", apply_mask_on_id=False
        )

        # Create new likes group with unmasked movies
        old_likes_group = self.likes_group
        likes_data_with_weight = old_likes_group.data
        new_likes_group = EdgePropertyGroup(
            "likes",
            likes_data_with_weight,
            old_likes_group.src_property_group,
            unmasked_movies_group,
            old_likes_group.is_directed,
            old_likes_group.src_column_name,
            old_likes_group.dst_column_name,
            old_likes_group.weight_column_name,
        )

        # Create modified graph
        modified_graph = PropertyGraphFrame(
            [self.people_group, unmasked_movies_group],
            [new_likes_group, self.messages_group],
        )

        graph = modified_graph.to_graph_frame(
            vertex_property_groups=["people", "movies"],
            edge_property_groups=["messages", "likes"],
            edge_group_filters={"messages": lit(True), "likes": lit(True)},
            vertex_group_filters={"people": lit(True), "movies": lit(True)},
        )

        vertices = {row.id for row in graph.vertices.collect()}
        edges = graph.edges.collect()

        # Verify movies vertices have original IDs
        self.assertIn("1", vertices)
        self.assertIn("2", vertices)
        self.assertIn("3", vertices)

        # Verify people vertices are masked
        self.assertIn(self.sha256_hash(1, "people"), vertices)

        # Verify edges have masked people IDs but original movie IDs
        likes_edges = [e for e in edges if e.weight == 1.0]
        self.assertTrue(
            any(
                e.src == self.sha256_hash(1, "people") and e.dst == "1"
                for e in likes_edges
            )
        )
        self.assertTrue(
            any(
                e.src == "1" and e.dst == self.sha256_hash(1, "people")
                for e in likes_edges
            )
        )

    def test_join_vertices_with_connected_components(self):
        """Test join_vertices with connected components."""
        # Convert to GraphFrame with all vertices and edges
        graph = self.people_movies_graph.to_graph_frame(
            vertex_property_groups=["people", "movies"],
            edge_property_groups=["messages", "likes"],
            edge_group_filters={"messages": lit(True), "likes": lit(True)},
            vertex_group_filters={"people": lit(True), "movies": lit(True)},
        )

        # Compute connected components
        components = graph.connectedComponents()

        # Join back
        joined_back = self.people_movies_graph.join_vertices(
            components, vertex_groups=["people", "movies"]
        )

        joined_data = joined_back.collect()

        # Group by property_group
        by_group = {}
        for row in joined_data:
            group = row.property_group
            if group not in by_group:
                by_group[group] = []
            by_group[group].append(row)

        self.assertIn("movies", by_group)
        self.assertIn("people", by_group)
        self.assertEqual(len(by_group["movies"]), 3)
        self.assertEqual(len(by_group["people"]), 5)

    def test_vertex_property_group_validation(self):
        """Test VertexPropertyGroup validation."""
        from graphframes.pg.property_groups import InvalidPropertyGroupException

        with self.assertRaises(InvalidPropertyGroupException):
            VertexPropertyGroup("test", self.people_group.data, "nonexistent_column")

    def test_edge_property_group_validation(self):
        """Test EdgePropertyGroup validation."""
        from graphframes.pg.property_groups import InvalidPropertyGroupException

        # Missing source column
        with self.assertRaises(InvalidPropertyGroupException):
            EdgePropertyGroup(
                "test",
                self.likes_group.data,
                self.people_group,
                self.movies_group,
                is_directed=True,
                src_column_name="nonexistent",
                dst_column_name="dst",
                weight_column_name="weight",
            )

        # Missing destination column
        with self.assertRaises(InvalidPropertyGroupException):
            EdgePropertyGroup(
                "test",
                self.likes_group.data,
                self.people_group,
                self.movies_group,
                is_directed=True,
                src_column_name="src",
                dst_column_name="nonexistent",
                weight_column_name="weight",
            )

        # Missing weight column
        with self.assertRaises(InvalidPropertyGroupException):
            EdgePropertyGroup(
                "test",
                self.likes_group.data,
                self.people_group,
                self.movies_group,
                is_directed=True,
                src_column_name="src",
                dst_column_name="dst",
                weight_column_name="nonexistent",
            )

    def test_to_graph_frame_invalid_group(self):
        """Test to_graph_frame with invalid group names."""
        with self.assertRaises(ValueError):
            self.people_movies_graph.to_graph_frame(
                vertex_property_groups=["nonexistent"],
                edge_property_groups=["likes"],
            )

        with self.assertRaises(ValueError):
            self.people_movies_graph.to_graph_frame(
                vertex_property_groups=["people"],
                edge_property_groups=["nonexistent"],
            )

    def test_projection_by_invalid_group(self):
        """Test projection_by with invalid group names."""
        with self.assertRaises(ValueError):
            self.people_movies_graph.projection_by("nonexistent", "movies", "likes")

        with self.assertRaises(ValueError):
            self.people_movies_graph.projection_by("people", "nonexistent", "likes")

        with self.assertRaises(ValueError):
            self.people_movies_graph.projection_by("people", "movies", "nonexistent")

    def test_property_graph_frame_to_graph_frame_conversion(self):
        """Test conversion from PropertyGraphFrame to GraphFrame and back concept."""
        graph = self.people_movies_graph.to_graph_frame(
            vertex_property_groups=["people"],
            edge_property_groups=["messages"],
        )

        self.assertIsInstance(graph, GraphFrame)
        self.assertIn(GraphFrame.ID, graph.vertices.columns)
        self.assertIn(GraphFrame.SRC, graph.edges.columns)
        self.assertIn(GraphFrame.DST, graph.edges.columns)
        self.assertIn(GraphFrame.WEIGHT, graph.edges.columns)


if __name__ == "__main__":
    unittest.main()
