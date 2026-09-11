package org.graphframes.propertygraph.internal

import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.PropertyGraphFrame
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

class SchemaGraphSnapshotSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  import sqlImplicits._

  test("toDOT returns valid and deterministic DOT output") {
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("movies", "people", "genres"),
      edges = Vector(
        SchemaEdge("likes", "people", "movies", true),
        SchemaEdge("belongs_to", "movies", "genres", true),
        SchemaEdge("follows", "people", "people", true)))

    val dot = SchemaGraphSnapshot.toDOT(snapshot)

    val expected =
      """digraph SchemaGraph {
        |  "genres";
        |  "movies";
        |  "people";
        |  "movies" -> "genres" [label="belongs_to"];
        |  "people" -> "movies" [label="likes"];
        |  "people" -> "people" [label="follows"];
        |}""".stripMargin

    assert(dot === expected)
  }

  test("toDOT escapes quotes and backslashes in names") {
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("v\"1", "path\\node"),
      edges = Vector(SchemaEdge("edge\"name", "v\"1", "path\\node", true)))

    val dot = SchemaGraphSnapshot.toDOT(snapshot)

    val expected =
      """digraph SchemaGraph {
        |  "path\\node";
        |  "v\"1";
        |  "v\"1" -> "path\\node" [label="edge\"name"];
        |}""".stripMargin

    assert(dot === expected)
  }

  test("fromPropertyGraphFrame builds empty schema snapshot") {
    val users = VertexPropertyGroup("users", Seq.empty[(Long)].toDF("id"), "id")
    val pgf = PropertyGraphFrame(Seq(users), Seq.empty)

    val snapshot = SchemaGraphSnapshot.fromPropertyGraphFrame(pgf)

    assert(snapshot.vertexGroupNames === Set("users"))
    assert(snapshot.edges === Vector.empty)
    assert(snapshot.outgoing === Map.empty)
    assert(snapshot.incoming === Map.empty)
  }

  test("toString returns human-readable and deterministic schema description") {
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("movies", "people", "genres"),
      edges = Vector(
        SchemaEdge("likes", "people", "movies", true),
        SchemaEdge("belongs_to", "movies", "genres", true),
        SchemaEdge("follows", "people", "people", true)))

    val description = SchemaGraphSnapshot.toString(snapshot)

    val expected =
      """Property graph schema:
        |Vertex property groups (3):
        |  - genres
        |  - movies
        |  - people
        |Edge property groups (3):
        |  - belongs_to: movies -> genres
        |  - likes: people -> movies
        |  - follows: people -> people""".stripMargin

    assert(description === expected)
  }

  test("toString renders empty graph schema sections") {
    val snapshot = SchemaGraphSnapshot(vertexGroupNames = Set.empty, edges = Vector.empty)

    val description = SchemaGraphSnapshot.toString(snapshot)

    val expected =
      """Property graph schema:
        |Vertex property groups (0):
        |  (none)
        |Edge property groups (0):
        |  (none)""".stripMargin

    assert(description === expected)
  }

  test("fromPropertyGraphFrame extracts vertex and edge group schema") {
    val users = VertexPropertyGroup("users", Seq.empty[(Long)].toDF("id"), "id")
    val posts = VertexPropertyGroup("posts", Seq.empty[(Long)].toDF("id"), "id")

    val writes = EdgePropertyGroup(
      name = "writes",
      data = Seq.empty[(Long, Long)].toDF("src", "dst"),
      srcPropertyGroup = users,
      dstPropertyGroup = posts,
      isDirected = true,
      srcColumnName = "src",
      dstColumnName = "dst",
      weightColumn = lit(1.0))

    val follows = EdgePropertyGroup(
      name = "follows",
      data = Seq.empty[(Long, Long, Double)].toDF("src", "dst", "weight"),
      srcPropertyGroup = users,
      dstPropertyGroup = users,
      isDirected = false,
      srcColumnName = GraphFrame.SRC,
      dstColumnName = GraphFrame.DST,
      weightColumnName = GraphFrame.WEIGHT)

    val pgf = PropertyGraphFrame(Seq(users, posts), Seq(writes, follows))

    val snapshot = SchemaGraphSnapshot.fromPropertyGraphFrame(pgf)

    assert(snapshot.vertexGroupNames === Set("users", "posts"))
    assert(
      snapshot.edges === Vector(
        SchemaEdge("writes", "users", "posts", true),
        SchemaEdge("follows", "users", "users", false)))

    assert(
      snapshot.outgoing === Map(
        "users" -> Vector(
          SchemaEdge("writes", "users", "posts", true),
          SchemaEdge("follows", "users", "users", false))))
    assert(
      snapshot.incoming === Map(
        "posts" -> Vector(SchemaEdge("writes", "users", "posts", true)),
        "users" -> Vector(SchemaEdge("follows", "users", "users", false))))
  }

  test("toDOT renders an undirected edge group with the same arrow as a directed one") {
    // DOT output is always a `digraph` and every edge is drawn `->`; direction lives in the
    // schema, not in the rendering. Pinned so a reader of the DOT is not misled into thinking
    // the renderer distinguishes them.
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("Person"),
      edges = Vector(
        SchemaEdge("KNOWS", "Person", "Person", isDirected = false),
        SchemaEdge("FOLLOWS", "Person", "Person", isDirected = true)))
    val dot = SchemaGraphSnapshot.toDOT(snapshot)
    assert(dot.contains("\"Person\" -> \"Person\" [label=\"KNOWS\"];"))
    assert(dot.contains("\"Person\" -> \"Person\" [label=\"FOLLOWS\"];"))
  }

  test("toString does not distinguish directed from undirected edge groups either") {
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("Person"),
      edges = Vector(SchemaEdge("KNOWS", "Person", "Person", isDirected = false)))
    assert(SchemaGraphSnapshot.toString(snapshot).contains("KNOWS: Person -> Person"))
  }

  test("outgoing and incoming index every edge exactly once per endpoint") {
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("Person", "Company"),
      edges = Vector(
        SchemaEdge("KNOWS", "Person", "Person", isDirected = true),
        SchemaEdge("WORKS_AT", "Person", "Company", isDirected = true)))
    assert(snapshot.outgoing("Person").map(_.edgeGroupName).toSet === Set("KNOWS", "WORKS_AT"))
    assert(snapshot.incoming("Person").map(_.edgeGroupName).toSet === Set("KNOWS"))
    assert(snapshot.incoming("Company").map(_.edgeGroupName).toSet === Set("WORKS_AT"))
    // A group with no edges in a direction is simply absent from the index.
    assert(snapshot.outgoing.get("Company") === None)
  }

  test("parallel edge groups between the same pair are both indexed") {
    val snapshot = SchemaGraphSnapshot(
      vertexGroupNames = Set("Person"),
      edges = Vector(
        SchemaEdge("KNOWS", "Person", "Person", isDirected = true),
        SchemaEdge("FOLLOWS", "Person", "Person", isDirected = true)))
    assert(snapshot.outgoing("Person").length === 2)
    assert(snapshot.incoming("Person").length === 2)
  }
}
