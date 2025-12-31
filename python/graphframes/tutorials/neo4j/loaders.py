"""APOC Parquet loading functions for Stack Exchange data."""

from __future__ import annotations

import glob
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j import Driver

# Neo4j import directory inside the container
NEO4J_IMPORT_DIR = "/var/lib/neo4j/import"


def get_container_path(host_path: str, data_dir: str) -> str:
    """Convert host path to container path for Neo4j APOC.

    Args:
        host_path: Full path on the host system
        data_dir: The data directory that is mounted at NEO4J_IMPORT_DIR

    Returns:
        file:// URI using the container's import directory path
    """
    # Get the path relative to the data_dir
    abs_data_dir = os.path.abspath(data_dir)
    abs_host_path = os.path.abspath(host_path)

    # Calculate relative path from data_dir
    rel_path = os.path.relpath(abs_host_path, abs_data_dir)

    # Construct container path
    container_path = os.path.join(NEO4J_IMPORT_DIR, rel_path)
    return f"file://{container_path}"


def get_parquet_file_uris(parquet_dir: str, data_dir: str) -> list[str]:
    """Get file:// URIs for all part files in a Spark parquet directory.

    Spark writes parquet as a directory containing part-*.parquet files.
    This function finds all part files and converts them to container URIs.

    Args:
        parquet_dir: Host path to the parquet directory (e.g., Users.parquet/)
        data_dir: The data directory mounted in the container

    Returns:
        List of file:// URIs for each part file
    """
    # Find all part files in the directory
    part_pattern = os.path.join(parquet_dir, "part-*.parquet")
    part_files = sorted(glob.glob(part_pattern))

    # Convert each to a container URI
    return [get_container_path(f, data_dir) for f in part_files]


def clear_database(driver: Driver) -> None:
    """Clear all nodes and relationships from the database."""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


def create_indexes(driver: Driver) -> None:
    """Create indexes on id and stackId fields for efficient matching."""
    indexes = [
        ("User", "id"),
        ("User", "stackId"),
        ("Question", "id"),
        ("Question", "stackId"),
        ("Answer", "id"),
        ("Answer", "stackId"),
        ("Post", "id"),
        ("Post", "stackId"),
        ("Vote", "id"),
        ("Vote", "stackId"),
        ("Tag", "id"),
        ("Tag", "stackId"),
        ("Badge", "id"),
        ("Badge", "stackId"),
        ("PostLink", "id"),
        ("PostLink", "stackId"),
        ("PostHistory", "id"),
        ("PostHistory", "stackId"),
        ("Comment", "id"),
        ("Comment", "stackId"),
    ]

    with driver.session() as session:
        for label, prop in indexes:
            try:
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{prop})")
            except Exception:
                pass  # Index may already exist


# =============================================================================
# Node Loaders - Each loads a specific node type with all its properties
# =============================================================================


def load_users(driver: Driver, file_uris: list[str]) -> int:
    """Load User nodes with all their properties.

    Properties: id, stackId, reputation, displayName, location, aboutMe,
                views, upVotes, downVotes, creationDate, lastAccessDate,
                websiteUrl, accountId
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (u:User {
        id: value.id,
        stackId: value.StackId,
        reputation: value.Reputation,
        creationDate: value.CreationDate,
        displayName: value.DisplayName,
        lastAccessDate: value.LastAccessDate,
        websiteUrl: value.WebsiteUrl,
        location: value.Location,
        aboutMe: value.AboutMe,
        views: value.Views,
        upVotes: value.UpVotes,
        downVotes: value.DownVotes,
        accountId: value.AccountId
    })
    RETURN count(u) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_questions(driver: Driver, file_uris: list[str]) -> int:
    """Load Question nodes with all their properties.

    Properties: id, stackId, title, body, score, viewCount, tags,
                ownerUserId, acceptedAnswerId, answerCount, commentCount,
                creationDate, lastEditDate, lastActivityDate, contentLicense
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (q:Question:Post {
        id: value.id,
        stackId: value.StackId,
        postTypeId: value.PostTypeId,
        acceptedAnswerId: value.AcceptedAnswerId,
        creationDate: value.CreationDate,
        score: value.Score,
        viewCount: value.ViewCount,
        body: value.Body,
        ownerUserId: value.OwnerUserId,
        lastEditorUserId: value.LastEditorUserId,
        lastEditDate: value.LastEditDate,
        lastActivityDate: value.LastActivityDate,
        title: value.Title,
        tags: value.Tags,
        answerCount: value.AnswerCount,
        commentCount: value.CommentCount,
        contentLicense: value.ContentLicense
    })
    RETURN count(q) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_answers(driver: Driver, file_uris: list[str]) -> int:
    """Load Answer nodes with all their properties.

    Properties: id, stackId, parentId, body, score, ownerUserId,
                creationDate, lastEditDate, lastActivityDate,
                commentCount, contentLicense
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (a:Answer:Post {
        id: value.id,
        stackId: value.StackId,
        postTypeId: value.PostTypeId,
        parentId: value.ParentId,
        creationDate: value.CreationDate,
        score: value.Score,
        body: value.Body,
        ownerUserId: value.OwnerUserId,
        lastEditorUserId: value.LastEditorUserId,
        lastEditDate: value.LastEditDate,
        lastActivityDate: value.LastActivityDate,
        commentCount: value.CommentCount,
        contentLicense: value.ContentLicense
    })
    RETURN count(a) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_votes(driver: Driver, file_uris: list[str]) -> int:
    """Load Vote nodes with all their properties.

    Properties: id, stackId, postId, voteTypeId, voteType, creationDate
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (v:Vote {
        id: value.id,
        stackId: value.StackId,
        postId: value.PostId,
        voteTypeId: value.VoteTypeId,
        voteType: value.VoteType,
        creationDate: value.CreationDate
    })
    RETURN count(v) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_tags(driver: Driver, file_uris: list[str]) -> int:
    """Load Tag nodes with all their properties.

    Properties: id, stackId, tagName, count, isRequired
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (t:Tag {
        id: value.id,
        stackId: value.StackId,
        tagName: value.TagName,
        count: value.Count,
        isRequired: value.IsRequired
    })
    RETURN count(t) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_badges(driver: Driver, file_uris: list[str]) -> int:
    """Load Badge nodes with all their properties.

    Properties: id, stackId, userId, name, date, class, tagBased
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (b:Badge {
        id: value.id,
        stackId: value.StackId,
        userId: value.UserId,
        name: value.Name,
        date: value.Date,
        class: value.Class,
        tagBased: value.TagBased
    })
    RETURN count(b) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_post_links(driver: Driver, file_uris: list[str]) -> int:
    """Load PostLink nodes with all their properties.

    Properties: id, stackId, postId, relatedPostId, linkTypeId, linkType, creationDate
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (pl:PostLink {
        id: value.id,
        stackId: value.StackId,
        creationDate: value.CreationDate,
        postId: value.PostId,
        relatedPostId: value.RelatedPostId,
        linkTypeId: value.LinkTypeId,
        linkType: value.LinkType
    })
    RETURN count(pl) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_post_history(driver: Driver, file_uris: list[str]) -> int:
    """Load PostHistory nodes with all their properties.

    Properties: id, stackId, postId, postHistoryTypeId, revisionGUID,
                userId, text, creationDate, contentLicense
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (ph:PostHistory {
        id: value.id,
        stackId: value.StackId,
        postHistoryTypeId: value.PostHistoryTypeId,
        postId: value.PostId,
        revisionGUID: value.RevisionGUID,
        creationDate: value.CreationDate,
        userId: value.UserId,
        text: value.Text,
        contentLicense: value.ContentLicense
    })
    RETURN count(ph) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_comments(driver: Driver, file_uris: list[str]) -> int:
    """Load Comment nodes with all their properties.

    Properties: id, stackId, postId, text, score, userId, creationDate
    """
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    CREATE (c:Comment {
        id: value.id,
        stackId: value.StackId,
        postId: value.PostId,
        score: value.Score,
        text: value.Text,
        creationDate: value.CreationDate,
        userId: value.UserId
    })
    RETURN count(c) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


# =============================================================================
# Edge Loaders - Each loads a specific relationship type
# =============================================================================


def load_edges_cast_for(driver: Driver, file_uris: list[str]) -> int:
    """Load CAST_FOR relationships (Vote -> Post)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:Vote {id: value.src})
    MATCH (dst:Post {id: value.dst})
    CREATE (src)-[r:CAST_FOR]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_asks(driver: Driver, file_uris: list[str]) -> int:
    """Load ASKS relationships (User -> Question)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:User {id: value.src})
    MATCH (dst:Question {id: value.dst})
    CREATE (src)-[r:ASKS]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_posts(driver: Driver, file_uris: list[str]) -> int:
    """Load POSTS relationships (User -> Answer)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:User {id: value.src})
    MATCH (dst:Answer {id: value.dst})
    CREATE (src)-[r:POSTS]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_answers(driver: Driver, file_uris: list[str]) -> int:
    """Load ANSWERS relationships (Answer -> Question)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:Answer {id: value.src})
    MATCH (dst:Question {id: value.dst})
    CREATE (src)-[r:ANSWERS]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_tags(driver: Driver, file_uris: list[str]) -> int:
    """Load TAGS relationships (Tag -> Post)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:Tag {id: value.src})
    MATCH (dst:Post {id: value.dst})
    CREATE (src)-[r:TAGS]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_earns(driver: Driver, file_uris: list[str]) -> int:
    """Load EARNS relationships (User -> Badge)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:User {id: value.src})
    MATCH (dst:Badge {id: value.dst})
    CREATE (src)-[r:EARNS]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_duplicates(driver: Driver, file_uris: list[str]) -> int:
    """Load DUPLICATES relationships (Post -> Post)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:Post {id: value.src})
    MATCH (dst:Post {id: value.dst})
    CREATE (src)-[r:DUPLICATES]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


def load_edges_links(driver: Driver, file_uris: list[str]) -> int:
    """Load LINKS relationships (Post -> Post)."""
    cypher = """
    CALL apoc.load.parquet($file_uri) YIELD value
    MATCH (src:Post {id: value.src})
    MATCH (dst:Post {id: value.dst})
    CREATE (src)-[r:LINKS]->(dst)
    RETURN count(r) as count
    """
    total = 0
    with driver.session() as session:
        for file_uri in file_uris:
            result = session.run(cypher, file_uri=file_uri)
            record = result.single()
            total += record["count"] if record else 0
    return total


# =============================================================================
# Loader Registry - Maps types to their loader functions
# =============================================================================

NODE_LOADERS = [
    ("User", "Users.parquet", load_users),
    ("Question", "Questions.parquet", load_questions),
    ("Answer", "Answers.parquet", load_answers),
    ("Vote", "Votes.parquet", load_votes),
    ("Tag", "Tags.parquet", load_tags),
    ("Badge", "Badges.parquet", load_badges),
    ("PostLink", "PostLinks.parquet", load_post_links),
    ("PostHistory", "PostHistory.parquet", load_post_history),
    ("Comment", "Comments.parquet", load_comments),
]

EDGE_LOADERS = [
    ("CAST_FOR", "CastFor.parquet", load_edges_cast_for),
    ("ASKS", "Asks.parquet", load_edges_asks),
    ("POSTS", "Posts.parquet", load_edges_posts),
    ("ANSWERS", "Answers.parquet", load_edges_answers),
    ("TAGS", "Tags.parquet", load_edges_tags),
    ("EARNS", "Earns.parquet", load_edges_earns),
    ("DUPLICATES", "Duplicates.parquet", load_edges_duplicates),
    ("LINKS", "Links.parquet", load_edges_links),
]
