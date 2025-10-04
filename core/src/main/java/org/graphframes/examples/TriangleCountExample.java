package org.graphframes.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import org.graphframes.lib.TriangleCount;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The TriangleCount class demonstrates how to use the GraphFrames library in Apache Spark
 * to count triangles in a graph dataset. A triangle in a graph is defined as a set of
 * three interconnected vertices.
 * <p>
 * This examples uses graphs from the LDBC Graphalytics benchmark datasets.
 * The first argument is the name of the benchmark dataset, the second argument is the path where datasets are stored.
 */
public class TriangleCountExample {
    public static void main(String[] args) {
        String benchmarkName;
        if (args.length > 0) {
            benchmarkName = args[0];
        } else {
            benchmarkName = "kgs";
        }

        Path resourcesPath;
        if (args.length > 1) {
            resourcesPath = Paths.get(args[1]);
        } else {
            resourcesPath = Paths.get("/tmp/ldbc_graphalitics_datesets");
        }

        Path caseRoot = resourcesPath.resolve(benchmarkName);
        SparkConf sparkConf = new SparkConf()
            .setAppName("TriangleCountExample")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        SparkContext context = spark.sparkContext();
        context.setLogLevel("ERROR");
        context.setCheckpointDir("/tmp/graphframes-checkpoints");

        LDBCUtils.downloadLDBCIfNotExists(resourcesPath, benchmarkName);
        StructField[] edgeFields = new StructField[]{
            new StructField("src", DataTypes.LongType, true, Metadata.empty()),
            new StructField("dst", DataTypes.LongType, true, Metadata.empty())
        };
        Dataset<Row> edges = spark.read()
            .format("csv")
            .option("header", "false")
            .option("delimiter", " ")
            .schema(new StructType(edgeFields))
            .load(caseRoot.resolve(benchmarkName + ".e").toString())
            .persist(StorageLevel.MEMORY_AND_DISK_SER());
        System.out.println("Edges loaded: " + edges.count());

        StructField[] vertexFields = new StructField[]{
            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
        };
        Dataset<Row> vertices = spark.read()
            .format("csv")
            .option("header", "false")
            .option("delimiter", " ")
            .schema(new StructType(vertexFields))
            .load(caseRoot.resolve(benchmarkName + ".v").toString())
            .persist(StorageLevel.MEMORY_AND_DISK_SER());
        System.out.println("Vertices loaded: " + vertices.count());

        long start = System.currentTimeMillis();
        GraphFrame graph = GraphFrame.apply(vertices, edges);
        TriangleCount counter = graph.triangleCount();
        Dataset<Row> triangles = counter.run();

        triangles.show(20, false);
        long triangleCount = triangles.select(functions.sum("count")).first().getLong(0);
        System.out.println("Found triangles: " + triangleCount);
        long end = System.currentTimeMillis();
        System.out.println("Total running time in seconds: " + (end - start) / 1000.0);
    }
}
