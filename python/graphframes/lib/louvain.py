import sys

if sys.version > "3":
    basestring = str

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W


class Louvain:
    def __init__(self, graph):
        self.nodes = graph.nodes.selectExpr("id", "id as community")
        self.edges = graph.edges.select("src", "dst", "weight")
        self.m = edges.groupBy().sum("weight").collect()[0][0]

    @staticmethod
    def change_communities(nodes, edges, m):
        nodes = nodes.alias("nodes")
        edges = edges.alias("edges")

        nodes_ki = edges.groupBy("src").agg(F.sum("weight").alias("ki"))

        potential_edges = edges.join(
            nodes, F.col("edges.dst") == F.col("nodes.id"), "left"
        ).select("edges.src", "edges.dst", "edges.weight", "nodes.community")

        nodes_ki_in = potential_edges.groupBy("src", "community").agg(
            F.sum("weight").alias("ki_in")
        )

        communities_sigma_tot = potential_edges.groupBy("community").agg(
            F.sum("weight").alias("sigma_tot")
        )

        nodes_potential_modularity_gain = (
            nodes_ki_in.join(nodes_ki, "src")
            .join(
                communities_sigma_tot,
                nodes_ki_in["community"] == communities_sigma_tot["community"],
                "left",
            )
            .select(
                nodes_ki_in["src"],
                nodes_ki_in["community"],
                "ki",
                "ki_in",
                F.coalesce(communities_sigma_tot["sigma_tot"], F.lit(0)).alias(
                    "sigma_tot"
                ),
            )
            .withColumn(
                "modularity_gain",
                F.col("ki_in") / (2 * m)
                - (F.col("sigma_tot") * F.col("ki") / (2 * m**2)),
            )
        )

        nodes_best_community = (
            nodes_potential_modularity_gain.withColumn(
                "rank",
                F.row_number().over(
                    W.partitionBy("src").orderBy(F.desc("modularity_gain"))
                ),
            )
            .filter(F.col("rank") == 1)
            .filter(F.col("modularity_gain") > 0)
            .selectExpr("src", "community as best_community")
        ).alias("nodes_best_community")

        nodes_with_new_community = nodes.join(
            nodes_best_community, nodes.id == nodes_best_community.src, "left"
        ).select(
            nodes.id,
            F.coalesce(
                nodes_best_community.best_community, nodes.community
            ).alias("community"),
        )

        return nodes_with_new_community

    @staticmethod
    def calculate_modularity(nodes, edges, m):
        nodes_ki = edges.groupBy("src").agg(F.sum("weight").alias("ki"))
        nodes_kj = edges.groupBy("dst").agg(F.sum("weight").alias("kj"))
        nodes_ci = nodes.selectExpr("id as src", "community as ci")
        nodes_cj = nodes.selectExpr("id as dst", "community as cj")

        edges = (
            edges.join(nodes_ki, "src", "left")
            .join(nodes_kj, "dst", "left")
            .join(nodes_ci, "src", "left")
            .join(nodes_cj, "dst", "left")
        )

        modularity = (
            edges.withColumn("Aij", F.col("weight"))
            .withColumn("ki_kj", F.col("ki") * F.col("kj"))
            .withColumn(
                "modularity",
                F.when(
                    F.col("ci") == F.col("cj"),
                    (F.col("Aij") - F.col("ki_kj") / (2 * m)) / m,
                ).otherwise(0),
            )
            .agg(F.sum("modularity"))
            .collect()[0][0]
        )
        return modularity

    def run(self):
        modularity = -1
        while True:
            new_nodes = self.change_communities(self.nodes, self.edges, self.m).cache()
            new_modularity = self.calculate_modularity(new_nodes, self.edges, self.m)
            if new_modularity - modularity < 0.001:
                break
            modularity = new_modularity
            self.nodes = new_nodes
        return new_nodes
