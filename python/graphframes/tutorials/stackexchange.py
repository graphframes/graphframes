"""Build a Graph out of the Stack Exchange Data Dump XML files."""

#
# Interactive Usage: pyspark --packages com.databricks:spark-xml_2.12:0.18.0
#
# Batch Usage: spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 python/graphframes/tutorials/stackexchange.py
#

from __future__ import annotations

import re

import click
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

# Change me if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"


#
# Some utility functions
#


def remove_prefix(df: DataFrame) -> DataFrame:
    """Remove the _ prefix present in the fields of the DataFrame"""
    field_names = [x.name for x in df.schema]
    new_field_names = [x[1:] for x in field_names]
    s = []

    # Substitute the old name for the new one
    for old, new in zip(field_names, new_field_names):
        s.append(F.col(old).alias(new))
    return df.select(s)


@F.udf(returnType=T.ArrayType(T.StringType()))
def split_tags(tags: str) -> list[str]:
    if not tags:
        return []
    # Remove < and > and split into array
    return re.findall(r"<([^>]+)>", tags)


#
# Initialize a SparkSession with case sensitivity
#

spark: SparkSession = SparkSession.builder.appName("Stack Exchange Graph Builder").getOrCreate()
spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints/stackexchange")

click.echo("Loading data for stats.meta.stackexchange.com ...")


#
# Load the Posts...
#
posts_df: DataFrame = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="posts")
    .load(f"{BASE_PATH}/Posts.xml")
)
click.echo(f"\nTotal Posts:       {posts_df.count():,}")

# Remove the _ prefix from field names
posts_df = remove_prefix(posts_df)

# Create a list of tags
posts_df = (
    posts_df.withColumn(
        "ParsedTags", F.split(F.regexp_replace(F.col("Tags"), "^\\||\\|$", ""), "\\|")
    )
    .drop("Tags")
    .withColumnRenamed("ParsedTags", "Tags")
)


#
# Building blocks: separate the questions and answers
#

# Do the questions look ok? Questions have NO parent ID and DO have a Title
questions_df: DataFrame = posts_df.filter(posts_df.ParentId.isNull())
questions_df = questions_df.withColumn("Type", F.lit("Question")).cache()
click.echo(f"\nTotal questions: {questions_df.count():,}\n")

questions_df.select("ParentId", "Title", "Body").show(10)

# Answers DO have a ParentId parent post and no Title
answers_df: DataFrame = posts_df.filter(posts_df.ParentId.isNotNull())
answers_df = answers_df.withColumn("Type", F.lit("Answer")).cache()
click.echo(f"\nTotal answers: {answers_df.count():,}\n")

answers_df.select("ParentId", "Title", "Body").show(10)


#
# Load the PostLinks...
#

post_links_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="postlinks")
    .load(f"{BASE_PATH}/PostLinks.xml")
)
click.echo(f"Total PostLinks:   {post_links_df.count():,}")

# Remove the _ prefix from field names
post_links_df = (
    remove_prefix(post_links_df)
    .withColumn(
        "LinkType",
        F.when(F.col("LinkTypeId") == 1, "Linked")
        .when(F.col("LinkTypeId") == 3, "Duplicate")
        .otherwise("Unknown"),
    )
    .withColumn("Type", F.lit("PostLinks"))
)


#
# Load the PostHistory...
#

post_history_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="posthistory")
    .load(f"{BASE_PATH}/PostHistory.xml")
)
click.echo(f"Total PostHistory: {post_history_df.count():,}")

# Remove the _ prefix from field names
post_history_df = remove_prefix(post_history_df).withColumn("Type", F.lit("PostHistory"))


#
# Load the Comments...
#

comments_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="comments")
    .load(f"{BASE_PATH}/Comments.xml")
)
click.echo(f"Total Comments:    {comments_df.count():,}")

# Remove the _ prefix from field names
comments_df = remove_prefix(comments_df).withColumn("Type", F.lit("Comment"))


#
# Load the Users...
#

users_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="users")
    .load(f"{BASE_PATH}/Users.xml")
)
click.echo(f"Total Users:       {users_df.count():,}")

# Remove the _ prefix from field names
users_df = remove_prefix(users_df).withColumn("Type", F.lit("User"))


#
# Load the Votes...
#

votes_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="votes")
    .load(f"{BASE_PATH}/Votes.xml")
)
click.echo(f"Total Votes:       {votes_df.count():,}")

# Remove the _ prefix from field names
votes_df = remove_prefix(votes_df).withColumn("Type", F.lit("Vote"))

# Add a VoteType column
votes_df = votes_df.withColumn(
    "VoteType",
    F.when(F.col("VoteTypeId") == 2, "UpVote")
    .when(F.col("VoteTypeId") == 3, "DownVote")
    .when(F.col("VoteTypeId") == 4, "Favorite")
    .when(F.col("VoteTypeId") == 5, "Close")
    .when(F.col("VoteTypeId") == 6, "Reopen")
    .when(F.col("VoteTypeId") == 7, "BountyStart")
    .when(F.col("VoteTypeId") == 8, "BountyClose")
    .when(F.col("VoteTypeId") == 9, "Deletion")
    .when(F.col("VoteTypeId") == 10, "Undeletion")
    .when(F.col("VoteTypeId") == 11, "Spam")
    .when(F.col("VoteTypeId") == 12, "InformModerator")
    .otherwise("Unknown"),
)


#
# Load the Tags...
#

tags_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="tags")
    .load(f"{BASE_PATH}/Tags.xml")
)
click.echo(f"Total Tags:        {tags_df.count():,}")

# Remove the _ prefix from field names
tags_df = remove_prefix(tags_df).withColumn("Type", F.lit("Tag"))


#
# Load the Badges...
#

badges_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="badges")
    .load(f"{BASE_PATH}/Badges.xml")
)
click.echo(f"Total Badges:      {badges_df.count():,}\n")

# Remove the _ prefix from field names
badges_df = remove_prefix(badges_df).withColumn("Type", F.lit("Badge"))


#
# Form the nodes from the UNION of posts, users, votes and their combined schemas
#

all_cols: list[tuple[str, T.StructField]] = list(
    set(
        list(zip(answers_df.columns, answers_df.schema))
        + list(zip(questions_df.columns, questions_df.schema))
        + list(zip(post_links_df.columns, post_links_df.schema))
        + list(zip(comments_df.columns, comments_df.schema))
        + list(zip(users_df.columns, users_df.schema))
        + list(zip(votes_df.columns, votes_df.schema))
        + list(zip(tags_df.columns, tags_df.schema))
        + list(zip(badges_df.columns, badges_df.schema))
    )
)
all_column_names: list[str] = sorted([x[0] for x in all_cols])


def add_missing_columns(df: DataFrame, all_cols: list[tuple[str, T.StructField]]) -> DataFrame:
    """Add any missing columns from any DataFrame among several we want to merge."""
    for col_name, schema_field in all_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(schema_field.dataType))
    return df


# Now apply this function to each of your DataFrames to get a consistent schema
# posts_df = add_missing_columns(posts_df, all_cols).select(all_column_names)
questions_df = add_missing_columns(questions_df, all_cols).select(all_column_names)
answers_df = add_missing_columns(answers_df, all_cols).select(all_column_names)
post_links_df = add_missing_columns(post_links_df, all_cols).select(all_column_names)
users_df = add_missing_columns(users_df, all_cols).select(all_column_names)
votes_df = add_missing_columns(votes_df, all_cols).select(all_column_names)
tags_df = add_missing_columns(tags_df, all_cols).select(all_column_names)
badges_df = add_missing_columns(badges_df, all_cols).select(all_column_names)
assert (
    set(questions_df.columns)
    == set(answers_df.columns)
    == set(post_links_df.columns)
    == set(users_df.columns)
    == set(votes_df.columns)
    == set(all_column_names)
    == set(tags_df.columns)
    == set(badges_df.columns)
)

# Now union them together and remove duplicates
nodes_df: DataFrame = (
    questions_df.unionByName(answers_df)
    .unionByName(post_links_df)
    .unionByName(users_df)
    .unionByName(votes_df)
    .unionByName(tags_df)
    .unionByName(badges_df)
    .distinct()
)
click.echo(f"Total distinct nodes: {nodes_df.count():,}")

# Now add a unique lowercase 'id' field - standard for GraphFrames - moving the original...
# Stack Exchange Id to StackId
nodes_df = nodes_df.withColumnRenamed("Id", "StackId").drop("Id")

# Update the column list...
if "Id" in all_column_names:
    all_column_names.remove("Id")
all_column_names += ["StackId"]
all_column_names = sorted(all_column_names)

# Add the UUID 'id' field for GraphFrames. It will go in edges as 'src' and 'dst'
nodes_df = nodes_df.withColumn("id", F.expr("uuid()")).select("id", *all_column_names)

# Now create posts - combined questions and answers for things that can apply to them both
posts_df = questions_df.unionByName(answers_df).cache()


#
# Store the nodes to disk, reload and cache
#

NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"

# Write to disk and load back again
nodes_df.write.mode("overwrite").parquet(NODES_PATH)
nodes_df = spark.read.parquet(NODES_PATH)

nodes_df.select("id", "Type").groupBy("Type").count().orderBy(F.col("count").desc()).show()

# +---------+------+
# |     Type| count|
# +---------+------+
# |    Badge|43,029|
# |     Vote|42,593|
# |     User|37,709|
# |   Answer| 2,978|
# | Question| 2,025|
# |PostLinks| 1,274|
# |      Tag|   143|
# +---------+------+

# Helps performance of GraphFrames' algorithms
nodes_df = nodes_df.cache()

# Make sure we have the right columns and cached data
posts_df = nodes_df.filter(nodes_df.Type.isin("Question", "Answer")).cache()
questions_df = nodes_df.filter(nodes_df.Type == "Question").cache()
answers_df = nodes_df.filter(nodes_df.Type == "Answer").cache()
post_links_df = nodes_df.filter(nodes_df.Type == "PostLinks").cache()
users_df = nodes_df.filter(nodes_df.Type == "User").cache()
votes_df = nodes_df.filter(nodes_df.Type == "Vote").cache()
tags_df = nodes_df.filter(nodes_df.Type == "Tag").cache()
badges_df = nodes_df.filter(nodes_df.Type == "Badge").cache()


#
# Build the edges DataFrame:
#
# * [Vote]--CastFor-->[Post]
# * [User]--Asks-->[Question]
# * [User]--Posts-->[Answer]
# * [Post]--Answers-->[Question]
# * [Tag]--Tags-->[Post]
# * [User]--Earns-->[Badge]
# * [Post]--Links-->[Post]
#
# Remember: 'src', 'dst' and 'relationship' are standard edge fields in GraphFrames
# Remember: we must produce src/dst based on lowercase 'id' UUID, not 'Id' which is Stack Overflow's integer.
#

#
# Create a [Vote]--CastFor-->[Post] edge... remember a Post is a Question or Answer
#

src_vote_df: DataFrame = votes_df.select(
    F.col("id").alias("src"),
    F.col("StackId").alias("VoteId"),
    # Everything has all the fields - should build from base records but need UUIDs
    F.col("PostId").alias("VotePostId"),
)
cast_for_edge_df: DataFrame = src_vote_df.join(
    posts_df, on=src_vote_df.VotePostId == posts_df.StackId, how="inner"
).select(
    # 'src' comes from the votes' 'id'
    "src",
    # 'dst' comes from the posts' 'id'
    F.col("id").alias("dst"),
    # All edges have a 'relationship' field
    F.lit("CastFor").alias("relationship"),
)
click.echo(f"Total CastFor edges: {cast_for_edge_df.count():,}")
click.echo(f"Percentage of linked votes: {cast_for_edge_df.count() / votes_df.count():.2%}\n")


#
# Create a [User]--Asks-->[Question] edge
#

questions_asked_df: DataFrame = questions_df.select(
    F.col("OwnerUserId").alias("QuestionUserId"),
    F.col("id").alias("dst"),
    F.lit("Asks").alias("relationship"),
)
user_asks_edges_df: DataFrame = questions_asked_df.join(
    users_df, on=questions_asked_df.QuestionUserId == users_df.StackId, how="inner"
).select(
    # 'src' comes from the users' 'id'
    F.col("id").alias("src"),
    # 'dst' comes from the posts' 'id'
    "dst",
    # All edges have a 'relationship' field
    "relationship",
)
click.echo(f"Total Asks edges: {user_asks_edges_df.count():,}")
click.echo(
    f"Percentage of asked questions linked to users: {user_asks_edges_df.count() / questions_df.count():.2%}\n"
)


#
# Create a [User]--Posts-->[Answer] edge.
#

user_answers_df: DataFrame = answers_df.select(
    F.col("OwnerUserId").alias("AnswerUserId"),
    F.col("id").alias("dst"),
    F.lit("Posts").alias("relationship"),
)
user_answers_edges_df = user_answers_df.join(
    users_df, on=user_answers_df.AnswerUserId == users_df.StackId, how="inner"
).select(
    # 'src' comes from the users' 'id'
    F.col("id").alias("src"),
    # 'dst' comes from the posts' 'id'
    "dst",
    # All edges have a 'relationship' field
    "relationship",
)
click.echo(f"Total User Answers edges: {user_answers_edges_df.count():,}")
click.echo(
    f"Percentage of answers linked to users: {user_answers_edges_df.count() / answers_df.count():.2%}\n"
)


#
# Create a [Answer]--Answers-->[Question] edge
#

src_answers_df: DataFrame = answers_df.select(
    F.col("id").alias("src"),
    F.col("StackId").alias("AnswerId"),
    F.col("ParentId").alias("AnswerParentId"),
)
question_answers_edges_df: DataFrame = src_answers_df.join(
    posts_df, on=src_answers_df.AnswerParentId == questions_df.StackId, how="inner"
).select(
    # 'src' comes from the answers' 'id'
    "src",
    # 'dst' comes from the posts' 'id'
    F.col("id").alias("dst"),
    # All edges have a 'relationship' field
    F.lit("Answers").alias("relationship"),
)
click.echo(f"Total Posts Answers edges: {question_answers_edges_df.count():,}")
click.echo(
    f"Percentage of linked answers: {question_answers_edges_df.count() / answers_df.count():.2%}\n"
)


#
# Create a [Tag]--Tags-->[Post] edge... remember a Post is a Question or Answer
#

src_tags_df: DataFrame = posts_df.select(
    F.col("id").alias("dst"),
    # First remove leading/trailing < and >, then split on "><"
    F.explode("Tags").alias("Tag"),
)
tags_edge_df: DataFrame = src_tags_df.join(
    tags_df, on=src_tags_df.Tag == tags_df.TagName, how="inner"
).select(
    # 'src' comes from the posts' 'id'
    F.col("id").alias("src"),
    # 'dst' comes from the tags' 'id'
    "dst",
    # All edges have a 'relationship' field
    F.lit("Tags").alias("relationship"),
)
click.echo(f"Total Tags edges: {tags_edge_df.count():,}")
click.echo(f"Percentage of linked tags: {tags_edge_df.count() / posts_df.count():.2%}\n")


#
# Create a [User]--Earns-->[Badge] edge
#

earns_edges_df: DataFrame = badges_df.select(
    F.col("UserId").alias("BadgeUserId"),
    F.col("id").alias("dst"),
    F.lit("Earns").alias("relationship"),
)
earns_edges_df = earns_edges_df.join(
    users_df, on=earns_edges_df.BadgeUserId == users_df.StackId, how="inner"
).select(
    # 'src' comes from the users' 'id'
    F.col("id").alias("src"),
    # 'dst' comes from the badges' 'id'
    "dst",
    # All edges have a 'relationship' field
    "relationship",
)
click.echo(f"Total Earns edges: {earns_edges_df.count():,}")
click.echo(f"Percentage of earned badges: {earns_edges_df.count() / badges_df.count():.2%}\n")


#
# Create a [Post]--Links-->[Post] edge... remember a Post is a Question or Answer
# Also a   [Post]--Duplicates-->[Post] edge... remember a Post is a Question or Answer
#

trim_links_df: DataFrame = post_links_df.select(
    F.col("PostId").alias("SrcPostId"),
    F.col("RelatedPostId").alias("DstPostId"),
    "LinkType",
)
links_src_edge_df: DataFrame = trim_links_df.join(
    posts_df.drop("LinkType"), on=trim_links_df.SrcPostId == posts_df.StackId, how="inner"
).select(
    # 'dst' comes from the posts' 'id'
    F.col("id").alias("src"),
    "DstPostId",
    "LinkType",
)
raw_links_edge_df = links_src_edge_df.join(
    posts_df.drop("LinkType"), on=links_src_edge_df.DstPostId == posts_df.StackId, how="inner"
).select(
    "src",
    # 'src' comes from the posts' 'id'
    F.col("id").alias("dst"),
    # All edges have a 'relationship' field
    F.lit("Links").alias("relationship"),
    "LinkType",
)

duplicates_edge_df: DataFrame = (
    raw_links_edge_df.filter(F.col("LinkType") == "Duplicate")
    .withColumn("relationship", F.lit("Duplicates"))
    .select("src", "dst", "relationship")
)
click.echo(f"Total Duplicates edges: {duplicates_edge_df.count():,}")
click.echo(
    f"Percentage of duplicate posts: {duplicates_edge_df.count() / post_links_df.count():.2%}\n"
)

linked_edge_df = (
    raw_links_edge_df.filter(F.col("LinkType") == "Linked")
    .withColumn("relationship", F.lit("Links"))
    .select("src", "dst", "relationship")
)
click.echo(f"Total Links edges: {linked_edge_df.count():,}")
click.echo(f"Percentage of linked posts: {linked_edge_df.count() / post_links_df.count():.2%}\n")


#
# Combine all the edges together into one relationships DataFrame
#

relationships_df: DataFrame = (
    cast_for_edge_df.unionByName(user_asks_edges_df)
    .unionByName(user_answers_edges_df)
    .unionByName(question_answers_edges_df)
    .unionByName(tags_edge_df)
    .unionByName(earns_edges_df)
    .unionByName(duplicates_edge_df)
    .unionByName(linked_edge_df)
)
relationships_df.groupBy("relationship").count().orderBy(F.col("count").desc()).withColumn(
    "count", F.format_number(F.col("count"), 0)
).show()


# +------------+------+
# |relationship| count|
# +------------+------+
# |       Earns|43,029|
# |     CastFor|40,701|
# |        Tags| 4,427|
# |     Answers| 2,978|
# |       Posts| 2,767|
# |        Asks| 1,934|
# |       Links| 1,180|
# |  Duplicates|    88|
# +------------+------+

EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"

# Write to disk and back again
relationships_df.write.mode("overwrite").parquet(EDGES_PATH)

spark.stop()
click.echo("Spark stopped.")
