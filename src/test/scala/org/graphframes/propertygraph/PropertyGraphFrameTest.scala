package org.graphframes.propertygraph

import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup
import org.scalatest.BeforeAndAfterAll

import java.security.MessageDigest

class PropertyGraphFrameTest
    extends SparkFunSuite
    with GraphFrameTestSparkContext
    with BeforeAndAfterAll {
  var peopleMoviesGraph: PropertyGraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // This graph represents a movie rating system with two types of vertices: 'people' (5 users: Alice, Bob, Charlie, David, Eve)
    // and 'movies' (3 movies: Matrix, Inception, Interstellar). The graph has two types of edges:
    // 1) 'likes' - undirected edges between people and movies with weight 1.0, representing movie preferences
    // 2) 'messages' - directed edges between people with varying weights (0.3-0.9), representing communication patterns.
    // The people-movie connections form a bipartite subgraph, while the messages form a directed cycle between users.

    val peopleData = spark
      .createDataFrame(
        Seq((1L, "Alice"), (2L, "Bob"), (3L, "Charlie"), (4L, "David"), (5L, "Eve")))
      .toDF("id", "name")

    val peopleGroup = VertexPropertyGroup("people", peopleData, "id")

    val moviesData = spark
      .createDataFrame(Seq((1L, "Matrix"), (2L, "Inception"), (3L, "Interstellar")))
      .toDF("id", "title")

    val moviesGroup = VertexPropertyGroup("movies", moviesData, "id")

    val likesData = spark
      .createDataFrame(Seq((1L, 1L), (1L, 2L), (2L, 1L), (3L, 2L), (4L, 3L), (5L, 2L)))
      .toDF("src", "dst")

    val likesGroup = EdgePropertyGroup(
      "likes",
      likesData,
      peopleGroup,
      moviesGroup,
      isDirected = false,
      "src",
      "dst",
      lit(1.0))

    val messagesData = spark
      .createDataFrame(
        Seq((1L, 2L, 5.0), (2L, 3L, 8.0), (3L, 4L, 3.0), (4L, 5L, 6.0), (5L, 1L, 9.0)))
      .toDF("src", "dst", "weight")

    val messagesGroup = EdgePropertyGroup(
      "messages",
      messagesData,
      peopleGroup,
      peopleGroup,
      isDirected = true,
      "src",
      "dst",
      col("weight"))

    peopleMoviesGraph =
      PropertyGraphFrame(Seq(peopleGroup, moviesGroup), Seq(likesGroup, messagesGroup))
  }

  test("projection by movies creates correct graph structure") {
    val projectedGraph = peopleMoviesGraph.projectionBy("people", "movies", "likes")

    assert(projectedGraph.vertexPropertyGroups.length === 1)
    assert(projectedGraph.vertexPropertyGroups.head.name === "people")

    assert(projectedGraph.edgesPropertyGroups.length === 2)
    assert(projectedGraph.edgesPropertyGroups.exists(_.name === "messages"))
    val projectedEdgesGroupOption =
      projectedGraph.edgesPropertyGroups.find(_.name === "projected_likes")

    assert(projectedEdgesGroupOption.isDefined)
    val projectedEdgesGroup = projectedEdgesGroupOption.get

    assert(projectedEdgesGroup.srcColumnName === GraphFrame.SRC)
    assert(projectedEdgesGroup.dstColumnName === GraphFrame.DST)
    assert(projectedEdgesGroup.weightColumnName === GraphFrame.WEIGHT)
    assert(!projectedEdgesGroup.isDirected)

    val projectedEdges = projectedEdgesGroup.data
      .collect()
      .map(row => (row.getLong(0), row.getLong(1)))
      .toSet

    // Expected edges between people who like the same movies
    val expectedEdges = Set(
      (1L, 2L), // Alice and Bob both like Matrix
      (1L, 3L), // Alice and Charlie both like Inception
      (1L, 5L), // Alice and Eve both like Inception
      (3L, 5L) // Charlie and Eve both like Inception
    )

    assert(projectedEdges === expectedEdges)
  }

  def sha256Hash(id: Long, groupName: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hash = md.digest(id.toString.getBytes("UTF-8")).map("%02x".format(_)).mkString
    s"$groupName$hash"
  }

  test("toGraphFrame with messages edges and people vertices only") {
    val graph = peopleMoviesGraph.toGraphFrame(
      Seq("people"),
      Seq("messages"),
      Map("messages" -> lit(true)),
      Map("people" -> lit(true)))

    val vertices = graph.vertices.collect().map(row => row.getString(0)).toSet
    val edges = graph.edges
      .collect()
      .map(row => (row.getString(0), row.getString(1), row.getDouble(2)))
      .toSet

    // Verify vertices (all people)
    val expectedVertices = Set(1L, 2L, 3L, 4L, 5L).map(sha256Hash(_, "people"))
    assert(vertices === expectedVertices)

    // Verify directed message edges with weights
    val expectedEdges =
      Set((1L, 2L, 5.0), (2L, 3L, 8.0), (3L, 4L, 3.0), (4L, 5L, 6.0), (5L, 1L, 9.0)).map {
        case (src, dst, weight) => (sha256Hash(src, "people"), sha256Hash(dst, "people"), weight)
      }
    assert(edges === expectedEdges)
  }

  test("toGraphFrame with all groups and proper edge handling") {
    val graph = peopleMoviesGraph.toGraphFrame(
      Seq("people", "movies"),
      Seq("messages", "likes"),
      Map("messages" -> lit(true), "likes" -> lit(true)),
      Map("people" -> lit(true), "movies" -> lit(true)))

    val vertices = graph.vertices.collect().toSet
    val edges = graph.edges.collect().toSet

    // Verify all vertices are present
    assert(vertices.size === 8) // 5 people + 3 movies

    // Verify vertex types are correctly preserved
    assert(vertices.count(_.getString(0) == sha256Hash(1L, "movies")) === 1)
    assert(vertices.count(_.getString(0) == sha256Hash(1L, "people")) === 1)

    // Verify edge counts and properties
    val messageEdges = edges.filter(_.getDouble(2) != 1.0)
    val likeEdges = edges.filter(_.getDouble(2) == 1.0)

    assert(messageEdges.size === 5) // Directed messages between people
    assert(likeEdges.size === 12) // 6 original edges * 2 (undirected converted to directed)

    // Verify undirected edges were properly converted to directed pairs
    val likesPairs = likeEdges.map(row => (row.getString(0), row.getString(1))).toSet
    assert(
      likesPairs.contains((sha256Hash(1, "people"), sha256Hash(1, "movies"))) &&
        likesPairs.contains((sha256Hash(1, "movies"), sha256Hash(1, "people"))))
    assert(
      likesPairs.contains((sha256Hash(1, "people"), sha256Hash(2, "movies"))) &&
        likesPairs.contains((sha256Hash(2, "movies"), sha256Hash(1, "people"))))
  }

}
