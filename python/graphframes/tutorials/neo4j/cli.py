"""CLI commands for Neo4j integration.

Usage:
    graphframes neo4j start
    graphframes neo4j load --subdomain stats.meta
    graphframes neo4j status
    graphframes neo4j stop
"""

from __future__ import annotations

import os
from pathlib import Path

import click
from neo4j import GraphDatabase

# Compute the default data directory relative to this package
_PACKAGE_DIR = Path(__file__).parent.parent  # tutorials/
DEFAULT_DATA_DIR = str(_PACKAGE_DIR / "data")

from graphframes.tutorials.neo4j.docker import (
    CONTAINER_NAME,
    DEFAULT_PASSWORD,
    check_docker,
    container_exists,
    remove_container,
    start_container,
    wait_for_ready,
)
from graphframes.tutorials.neo4j.loaders import (
    EDGE_LOADERS,
    NODE_LOADERS,
    clear_database,
    create_indexes,
    get_parquet_file_uris,
)


@click.group()
def neo4j():
    """Neo4j integration commands for GraphFrames."""
    pass


@neo4j.command()
@click.option(
    "--data-dir",
    default=DEFAULT_DATA_DIR,
    help="Directory containing the Parquet files (will be mounted for APOC access)",
)
@click.option(
    "--password",
    default=DEFAULT_PASSWORD,
    help="Neo4j password",
)
@click.option(
    "--heap-initial",
    default="1G",
    help="Initial heap size",
)
@click.option(
    "--heap-max",
    default="2G",
    help="Maximum heap size",
)
def start(data_dir: str, password: str, heap_initial: str, heap_max: str) -> None:
    """Start a Neo4j Docker container with APOC enabled.

    This command starts a Neo4j container configured for loading Stack Exchange
    data using APOC. If a container with the same name already exists, it will
    be stopped and removed first.

    The data directory is mounted into the container so APOC can access the
    Parquet files for loading.

    Example:
        graphframes neo4j start
        graphframes neo4j start --data-dir ./my-data --password mypassword
    """
    click.echo("=" * 70)
    click.echo("STARTING NEO4J DOCKER CONTAINER")
    click.echo("=" * 70)

    # Check if Docker is available
    if not check_docker():
        click.echo("\nError: Docker is not installed or not running.", err=True)
        click.echo("Please install Docker: https://docs.docker.com/get-docker/", err=True)
        raise click.Abort()

    # Stop and remove existing container if it exists
    click.echo(f"\n[1/3] Checking for existing container '{CONTAINER_NAME}'...")
    if container_exists():
        click.echo("  Removing existing container...")
        remove_container()
        click.echo("  ✓ Existing container removed")
    else:
        click.echo("  No existing container found")

    # Get absolute path for data directory
    abs_data_dir = os.path.abspath(data_dir)
    click.echo(f"\n[2/3] Starting Neo4j container...")
    click.echo(f"  Data directory: {abs_data_dir}")

    success, result = start_container(data_dir, password, heap_initial, heap_max)
    if not success:
        click.echo(f"\nError starting container: {result}", err=True)
        raise click.Abort()

    click.echo(f"  ✓ Container started: {result}")

    # Wait for Neo4j to be ready
    click.echo("\n[3/3] Waiting for Neo4j to be ready...")
    if wait_for_ready(password=password):
        click.echo("  ✓ Neo4j is ready!")
    else:
        click.echo("\n  Neo4j is still starting up. Check logs with:")
        click.echo(f"    docker logs {CONTAINER_NAME}")

    click.echo("\n" + "=" * 70)
    click.echo("✓ NEO4J CONTAINER STARTED")
    click.echo("=" * 70)
    click.echo("\nConnection details:")
    click.echo(f"  • Browser: http://localhost:7474 (neo4j / {password})")
    click.echo(f"  • Bolt URI: bolt://localhost:7687 (neo4j / {password})")
    click.echo(f"\nData mounted at: /var/lib/neo4j/import")
    click.echo("\nNext steps:")
    click.echo("  1. Run: graphframes neo4j load --subdomain stats.meta")
    click.echo("  2. Or use: graphframes neo4j status")


@neo4j.command()
def stop() -> None:
    """Stop and remove the Neo4j Docker container.

    Example:
        graphframes neo4j stop
    """
    click.echo(f"Stopping container '{CONTAINER_NAME}'...")

    if not container_exists():
        click.echo("No container found.")
        return

    remove_container()
    click.echo(f"✓ Container '{CONTAINER_NAME}' stopped and removed.")


@neo4j.command()
@click.option(
    "--subdomain",
    default="stats.meta",
    help="Stack Exchange subdomain (e.g., 'stats.meta' for stats.meta.stackexchange.com)",
)
@click.option(
    "--data-dir",
    default=DEFAULT_DATA_DIR,
    help="Directory containing the Parquet files",
)
@click.option(
    "--neo4j-uri",
    default="bolt://localhost:7687",
    help="Neo4j connection URI",
)
@click.option(
    "--neo4j-user",
    default="neo4j",
    help="Neo4j username",
)
@click.option(
    "--neo4j-password",
    default=DEFAULT_PASSWORD,
    help="Neo4j password",
)
@click.option(
    "--clear/--no-clear",
    default=True,
    help="Clear existing data before loading",
)
def load(
    subdomain: str,
    data_dir: str,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    clear: bool,
) -> None:
    """Load Stack Exchange Parquet data into Neo4j using APOC.

    This command loads the individual Parquet files generated by the stackexchange
    script into Neo4j using the APOC library's parquet loading capabilities.

    Each node type is loaded with all its original properties from the Stack Exchange
    data dump, preserving both the GraphFrames UUID 'id' and the original Stack Exchange
    'stackId' for reference.

    Prerequisites:
        - Neo4j running with APOC Extended plugin installed
        - Run stackexchange.py first to generate Parquet files

    Example:
        graphframes neo4j load --subdomain stats.meta
        graphframes neo4j load --subdomain stats.meta --neo4j-uri bolt://localhost:7687
    """
    site = f"{subdomain}.stackexchange.com"
    base_path = os.path.join(data_dir, site)

    click.echo("=" * 70)
    click.echo("LOADING STACK EXCHANGE DATA INTO NEO4J USING APOC")
    click.echo("=" * 70)
    click.echo(f"\nData source: {base_path}")
    click.echo(f"Neo4j URI: {neo4j_uri}")

    # Check that the data directory exists
    if not os.path.exists(base_path):
        click.echo(f"\nError: Data directory not found: {base_path}", err=True)
        click.echo("Please run the following first:", err=True)
        click.echo(f"  1. graphframes stackexchange {subdomain}", err=True)
        click.echo("  2. python python/graphframes/tutorials/stackexchange.py", err=True)
        raise click.Abort()

    # Check for required Parquet files
    required_files = ["Users.parquet", "Questions.parquet", "Answers.parquet"]
    missing_files = [f for f in required_files if not os.path.exists(os.path.join(base_path, f))]
    if missing_files:
        click.echo(f"\nError: Missing Parquet files: {missing_files}", err=True)
        click.echo("Please run: python python/graphframes/tutorials/stackexchange.py", err=True)
        raise click.Abort()

    # Connect to Neo4j
    click.echo("\n[1/5] Connecting to Neo4j...")
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        driver.verify_connectivity()
        click.echo("  ✓ Connected to Neo4j")
    except Exception as e:
        click.echo(f"\nError: Could not connect to Neo4j: {e}", err=True)
        click.echo("Please ensure Neo4j is running and credentials are correct.", err=True)
        raise click.Abort()

    # Clear existing data if requested
    if clear:
        click.echo("\n[2/5] Clearing existing data...")
        clear_database(driver)
        click.echo("  ✓ Database cleared")
    else:
        click.echo("\n[2/5] Skipping database clear (--no-clear)")

    # Create indexes
    click.echo("\n[3/5] Creating indexes...")
    create_indexes(driver)
    click.echo("  ✓ Indexes created")

    # Load nodes
    click.echo("\n[4/5] Loading nodes with properties...")
    total_nodes = 0
    for node_type, filename, loader_func in NODE_LOADERS:
        parquet_dir = os.path.join(base_path, filename)
        if os.path.exists(parquet_dir):
            try:
                file_uris = get_parquet_file_uris(parquet_dir, data_dir)
                if not file_uris:
                    click.echo(f"  - {node_type}: skipped (no part files found)")
                    continue
                count = loader_func(driver, file_uris)
                total_nodes += count
                click.echo(f"  ✓ {node_type}: {count:,} nodes ({len(file_uris)} part files)")
            except Exception as e:
                click.echo(f"  ✗ {node_type}: Failed - {e}")
        else:
            click.echo(f"  - {node_type}: skipped (directory not found)")

    # Load edges
    click.echo("\n[5/5] Loading relationships...")
    edges_dir = os.path.join(base_path, "edges")
    total_edges = 0
    for rel_type, filename, loader_func in EDGE_LOADERS:
        parquet_dir = os.path.join(edges_dir, filename)
        if os.path.exists(parquet_dir):
            try:
                file_uris = get_parquet_file_uris(parquet_dir, data_dir)
                if not file_uris:
                    click.echo(f"  - {rel_type}: skipped (no part files found)")
                    continue
                count = loader_func(driver, file_uris)
                total_edges += count
                click.echo(f"  ✓ {rel_type}: {count:,} relationships ({len(file_uris)} part files)")
            except Exception as e:
                click.echo(f"  ✗ {rel_type}: Failed - {e}")
        else:
            click.echo(f"  - {rel_type}: skipped (directory not found)")

    driver.close()

    click.echo("\n" + "=" * 70)
    click.echo("✓ DATA LOAD COMPLETE!")
    click.echo("=" * 70)
    click.echo(f"\nLoaded:")
    click.echo(f"  • {total_nodes:,} nodes")
    click.echo(f"  • {total_edges:,} relationships")
    click.echo("\nNode types and their properties:")
    click.echo("  • User: id, stackId, reputation, displayName, location, views, ...")
    click.echo("  • Question: id, stackId, title, body, score, viewCount, tags, ...")
    click.echo("  • Answer: id, stackId, body, score, parentId, ...")
    click.echo("  • Vote: id, stackId, voteType, postId, ...")
    click.echo("  • Tag: id, stackId, tagName, count, isRequired")
    click.echo("  • Badge: id, stackId, name, class, tagBased, ...")
    click.echo("  • PostLink: id, stackId, linkType, postId, relatedPostId, ...")
    click.echo("  • PostHistory: id, stackId, postHistoryTypeId, text, ...")
    click.echo("  • Comment: id, stackId, text, score, postId, userId")
    click.echo("\nNext steps:")
    click.echo(f"  1. Verify in Neo4j Browser: http://localhost:7474 (neo4j / {neo4j_password})")
    click.echo("  2. Run: MATCH (n) RETURN labels(n)[0] as Type, count(*) ORDER BY count(*) DESC")
    click.echo(
        "  3. Explore: MATCH (u:User)-[:ASKS]->(q:Question) RETURN u.displayName, q.title LIMIT 10"
    )


@neo4j.command()
@click.option(
    "--neo4j-uri",
    default="bolt://localhost:7687",
    help="Neo4j connection URI",
)
@click.option(
    "--neo4j-user",
    default="neo4j",
    help="Neo4j username",
)
@click.option(
    "--neo4j-password",
    default=DEFAULT_PASSWORD,
    help="Neo4j password",
)
def status(neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> None:
    """Check Neo4j connection and show database statistics."""
    click.echo("Checking Neo4j connection...")

    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        driver.verify_connectivity()
        click.echo(f"✓ Connected to Neo4j at {neo4j_uri}")

        with driver.session() as session:
            # Get node counts by label
            result = session.run(
                """
                MATCH (n)
                RETURN labels(n)[0] as label, count(*) as count
                ORDER BY count DESC
                """
            )
            records = list(result)

            if records:
                click.echo("\nNode counts by label:")
                for record in records:
                    click.echo(f"  {record['label']}: {record['count']:,}")
            else:
                click.echo("\nNo nodes found in database.")

            # Get relationship counts
            result = session.run(
                """
                MATCH ()-[r]->()
                RETURN type(r) as type, count(*) as count
                ORDER BY count DESC
                """
            )
            records = list(result)

            if records:
                click.echo("\nRelationship counts by type:")
                for record in records:
                    click.echo(f"  {record['type']}: {record['count']:,}")
            else:
                click.echo("\nNo relationships found in database.")

        driver.close()

    except Exception as e:
        click.echo(f"✗ Could not connect to Neo4j: {e}", err=True)
        raise click.Abort()
