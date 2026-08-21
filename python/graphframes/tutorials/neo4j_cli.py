#!/usr/bin/env python

"""Manage the tutorial's Neo4j container: `graphframes neo4j setup|load|remove`.

Companion to the Neo4j Integration Tutorial and to neo4j.py, which runs the full
round trip. These commands just handle the boring parts: starting a container,
loading the Stack Exchange graph into it, and tearing both down again.

Only `load` needs PySpark, so it imports it lazily - `setup` and `remove` work
with nothing but Docker installed.
"""

import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Optional

import click

CONTAINER_NAME = "neo4j-graphframes"
IMAGE = "neo4j:community"
DATA_ROOT = "/tmp/neo4j-data"
DEFAULT_PASSWORD = "graphframes123"
DEFAULT_USER = "neo4j"
DEFAULT_DATABASE = "neo4j"
DEFAULT_SITE = "stats.meta.stackexchange.com"

# Subdirectories bind-mounted into the container.
VOLUMES = {
    "data": "/data",
    "logs": "/logs",
    "import": "/var/lib/neo4j/import",
    "plugins": "/plugins",
}


def _require_docker() -> None:
    """Fail with a useful message if the Docker CLI is missing or the daemon is down."""
    if shutil.which("docker") is None:
        raise click.ClickException(
            "docker not found on PATH. Install Docker Desktop from https://www.docker.com/"
        )
    probe = subprocess.run(["docker", "info"], capture_output=True, text=True)
    if probe.returncode != 0:
        raise click.ClickException(
            "the Docker daemon is not responding. Start Docker Desktop and try again."
        )


def _docker(args: List[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a docker command, capturing output."""
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and result.returncode != 0:
        raise click.ClickException(
            f"docker {' '.join(args)} failed:\n{result.stderr.strip() or result.stdout.strip()}"
        )
    return result


def _container_state(name: str) -> Optional[str]:
    """Return the container's state ('running', 'exited', ...), or None if it does not exist."""
    result = _docker(
        ["ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.State}}"], check=False
    )
    state = result.stdout.strip()
    return state or None


def _wait_for_neo4j(name: str, user: str, password: str, timeout: int) -> bool:
    """Poll until Neo4j answers a trivial Cypher query, or the timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        probe = _docker(
            ["exec", name, "cypher-shell", "-u", user, "-p", password, "RETURN 1;"],
            check=False,
        )
        if probe.returncode == 0:
            return True
        time.sleep(2)
    return False


@click.group()
def neo4j() -> None:
    """Set up, load and tear down the tutorial's Neo4j container."""


@neo4j.command()
@click.option("--container-name", default=CONTAINER_NAME, help="Name for the Docker container")
@click.option("--data-root", default=DATA_ROOT, help="Host directory for Neo4j's volumes")
@click.option("--password", default=DEFAULT_PASSWORD, help="Password for the neo4j user")
@click.option("--http-port", default=7474, help="Host port to map to Neo4j's HTTP port")
@click.option("--bolt-port", default=7687, help="Host port to map to Neo4j's Bolt port")
@click.option("--heap", default="2G", help="Max JVM heap for Neo4j")
@click.option("--timeout", default=120, help="Seconds to wait for Neo4j to accept queries")
@click.option("--wait/--no-wait", default=True, help="Wait until Neo4j is ready before returning")
def setup(
    container_name: str,
    data_root: str,
    password: str,
    http_port: int,
    bolt_port: int,
    heap: str,
    timeout: int,
    wait: bool,
) -> None:
    """Start a Neo4j container for the tutorial.

    Safe to re-run: an existing container is started rather than replaced.

    Example: graphframes neo4j setup --password hunter2
    """
    _require_docker()

    state = _container_state(container_name)
    if state == "running":
        click.echo(f"Container '{container_name}' is already running.")
    elif state is not None:
        click.echo(f"Container '{container_name}' exists ({state}); starting it...")
        _docker(["start", container_name])
    else:
        for subdir in VOLUMES:
            Path(data_root, subdir).mkdir(parents=True, exist_ok=True)

        click.echo(f"Starting {IMAGE} as '{container_name}'...")
        run_args = [
            "run",
            "-d",
            "--name",
            container_name,
            "-p",
            f"{http_port}:7474",
            "-p",
            f"{bolt_port}:7687",
        ]
        for subdir, mount in VOLUMES.items():
            run_args += ["-v", f"{Path(data_root, subdir)}:{mount}"]
        run_args += [
            "-e",
            f"NEO4J_AUTH={DEFAULT_USER}/{password}",
            "-e",
            'NEO4J_PLUGINS=["apoc"]',
            "-e",
            "NEO4J_apoc_import_file_enabled=true",
            "-e",
            "NEO4J_apoc_export_file_enabled=true",
            "-e",
            "NEO4J_dbms_memory_heap_initial__size=1G",
            "-e",
            f"NEO4J_dbms_memory_heap_max__size={heap}",
            IMAGE,
        ]
        _docker(run_args)

    if wait:
        click.echo("Waiting for Neo4j to accept queries...")
        if not _wait_for_neo4j(container_name, DEFAULT_USER, password, timeout):
            raise click.ClickException(
                f"Neo4j did not become ready within {timeout}s. "
                f"Check the logs with: docker logs {container_name}"
            )
        click.echo("  ✓ ready")

    click.echo("")
    click.echo(f"  Browser: http://localhost:{http_port}")
    click.echo(f"  Bolt:    neo4j://localhost:{bolt_port}")
    click.echo(f"  Login:   {DEFAULT_USER} / {password}")
    click.echo("")
    click.echo("Next: graphframes neo4j load")


@neo4j.command()
@click.option(
    "--data-dir",
    default=None,
    help="Directory containing Stack Exchange Parquet data (default: package data directory)",
)
@click.option("--site", default=DEFAULT_SITE, help="Stack Exchange site subdirectory to load")
@click.option("--neo4j-url", default="neo4j://localhost:7687", help="Neo4j Bolt URL")
@click.option("--neo4j-user", default=DEFAULT_USER, help="Neo4j username")
@click.option("--neo4j-password", default=DEFAULT_PASSWORD, help="Neo4j password")
@click.option("--neo4j-database", default=DEFAULT_DATABASE, help="Neo4j database name")
def load(
    data_dir: Optional[str],
    site: str,
    neo4j_url: str,
    neo4j_user: str,
    neo4j_password: str,
    neo4j_database: str,
) -> None:
    """Load the Stack Exchange graph into Neo4j.

    Requires the Parquet built by `graphframes stackexchange` plus stackexchange.py.
    Idempotent: both loads MERGE, so re-running updates rather than duplicating.

    Example: graphframes neo4j load --site stats.meta.stackexchange.com
    """
    # Imported here so `setup` and `remove` do not require PySpark.
    from pyspark.sql import DataFrame, SparkSession

    from graphframes.tutorials.neo4j import (
        DEFAULT_DATA_DIR,
        GRAPHFRAMES_PACKAGE,
        NEO4J_PACKAGE,
        SHARED_LABEL,
        create_constraint,
        cypher,
        load_nodes,
        load_relationships,
    )

    base_path = f"{data_dir or DEFAULT_DATA_DIR}/{site}"
    if not Path(base_path, "Nodes.parquet").exists():
        raise click.ClickException(
            f"no Nodes.parquet under {base_path}\n"
            f"Build it first:\n"
            f"  graphframes stackexchange {site.split('.stackexchange.com')[0]}\n"
            f"  spark-submit python/graphframes/tutorials/stackexchange.py"
        )

    spark: SparkSession = (
        SparkSession.builder.appName("graphframes neo4j load")
        .config("spark.jars.packages", f"{GRAPHFRAMES_PACKAGE},{NEO4J_PACKAGE}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    neo4j_options = {
        "url": neo4j_url,
        "authentication.basic.username": neo4j_user,
        "authentication.basic.password": neo4j_password,
        "database": neo4j_database,
    }

    try:
        nodes_df: DataFrame = spark.read.parquet(f"{base_path}/Nodes.parquet").cache()
        edges_df: DataFrame = spark.read.parquet(f"{base_path}/Edges.parquet").cache()
        click.echo(f"Read {nodes_df.count():,} nodes and {edges_df.count():,} edges from Parquet")

        click.echo("\n=== Loading nodes ===")
        create_constraint(spark, neo4j_options)
        click.echo(f"  ✓ uniqueness constraint on :{SHARED_LABEL}(id)")
        load_nodes(nodes_df, neo4j_options)

        click.echo("\n=== Loading relationships ===")
        load_relationships(edges_df, neo4j_options)

        click.echo("\n=== Verifying ===")
        cypher(
            spark,
            neo4j_options,
            f"MATCH (n:{SHARED_LABEL}) RETURN n.Type AS Type, count(*) AS count "
            "ORDER BY count DESC",
        ).show()
        cypher(
            spark,
            neo4j_options,
            f"MATCH (:{SHARED_LABEL})-[r]->(:{SHARED_LABEL}) "
            "RETURN type(r) AS relationship, count(*) AS count ORDER BY count DESC",
        ).show()
    finally:
        spark.stop()

    click.echo("Loaded. Run the full round trip with:")
    click.echo("  spark-submit python/graphframes/tutorials/neo4j.py")


@neo4j.command()
@click.option("--container-name", default=CONTAINER_NAME, help="Name of the Docker container")
@click.option("--data-root", default=DATA_ROOT, help="Host directory holding Neo4j's volumes")
@click.option(
    "--keep-data/--delete-data",
    default=False,
    help="Keep the host data directory instead of deleting it",
)
@click.option("--yes", is_flag=True, help="Do not prompt for confirmation")
def remove(container_name: str, data_root: str, keep_data: bool, yes: bool) -> None:
    """Stop and delete the Neo4j container, and its data directory.

    This throws away the loaded graph. Re-create it with `setup` and `load`.

    Example: graphframes neo4j remove --yes
    """
    _require_docker()

    state = _container_state(container_name)
    targets = []
    if state is not None:
        targets.append(f"container '{container_name}' ({state})")
    if not keep_data and Path(data_root).exists():
        targets.append(f"data directory {data_root}")

    if not targets:
        click.echo("Nothing to remove.")
        return

    click.echo("This will permanently delete:")
    for target in targets:
        click.echo(f"  - {target}")
    if not yes:
        click.confirm("Continue?", abort=True)

    if state is not None:
        if state == "running":
            click.echo(f"Stopping '{container_name}'...")
            _docker(["stop", container_name])
        click.echo(f"Removing '{container_name}'...")
        _docker(["rm", container_name])
        click.echo("  ✓ container removed")

    if not keep_data:
        root = Path(data_root)
        if root.exists():
            click.echo(f"Deleting {root}...")
            try:
                shutil.rmtree(root)
            except PermissionError as error:
                # Neo4j writes as its own uid, so some files can be owned by another user.
                raise click.ClickException(
                    f"could not delete {root}: {error}\nRemove it manually: sudo rm -rf {root}"
                )
            click.echo("  ✓ data deleted")


if __name__ == "__main__":
    neo4j()
