"""Docker container management for Neo4j with APOC."""

from __future__ import annotations

import os
import subprocess
import time

from neo4j import GraphDatabase

# Default container configuration
CONTAINER_NAME = "neo4j-graphframes"
DEFAULT_PASSWORD = "graphframes123"
DEFAULT_IMAGE = "neo4j:community"

# APOC Extended JAR URL for parquet support
APOC_EXTENDED_VERSION = "5.26.0"
APOC_EXTENDED_URL = f"https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/{APOC_EXTENDED_VERSION}/apoc-{APOC_EXTENDED_VERSION}-extended.jar"


def check_docker() -> bool:
    """Check if Docker is available."""
    try:
        subprocess.run(
            ["docker", "--version"],
            check=True,
            capture_output=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def container_exists() -> bool:
    """Check if the Neo4j container exists."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name={CONTAINER_NAME}", "--format", "{{.ID}}"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def container_running() -> bool:
    """Check if the Neo4j container is running."""
    result = subprocess.run(
        ["docker", "ps", "--filter", f"name={CONTAINER_NAME}", "--format", "{{.ID}}"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def remove_container() -> bool:
    """Remove the existing container if it exists."""
    if container_exists():
        result = subprocess.run(
            ["docker", "rm", "-f", CONTAINER_NAME],
            capture_output=True,
        )
        return result.returncode == 0
    return True


def download_apoc_extended(plugins_dir: str) -> bool:
    """Download APOC Extended JAR for parquet support."""
    import urllib.request

    jar_path = os.path.join(plugins_dir, f"apoc-{APOC_EXTENDED_VERSION}-extended.jar")

    # Skip if already downloaded
    if os.path.exists(jar_path):
        return True

    os.makedirs(plugins_dir, exist_ok=True)

    try:
        urllib.request.urlretrieve(APOC_EXTENDED_URL, jar_path)
        return True
    except Exception:
        return False


def start_container(
    data_dir: str,
    password: str = DEFAULT_PASSWORD,
    heap_initial: str = "1G",
    heap_max: str = "2G",
) -> tuple[bool, str]:
    """Start a Neo4j container with APOC Extended enabled.

    Args:
        data_dir: Directory containing Parquet files (will be mounted)
        password: Neo4j password
        heap_initial: Initial heap size
        heap_max: Maximum heap size

    Returns:
        Tuple of (success, container_id or error message)
    """
    abs_data_dir = os.path.abspath(data_dir)
    plugins_dir = "/tmp/neo4j-data/plugins"

    # Download APOC Extended for parquet support
    if not download_apoc_extended(plugins_dir):
        return False, "Failed to download APOC Extended"

    docker_cmd = [
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", "7474:7474",
        "-p", "7687:7687",
        "-v", "/tmp/neo4j-data/data:/data",
        "-v", "/tmp/neo4j-data/logs:/logs",
        "-v", f"{abs_data_dir}:/var/lib/neo4j/import",
        "-v", f"{plugins_dir}:/plugins",
        "-e", "NEO4J_apoc_import_file_enabled=true",
        "-e", "NEO4J_apoc_export_file_enabled=true",
        "-e", "NEO4J_apoc_import_file_use__neo4j__config=false",
        "-e", f"NEO4J_AUTH=neo4j/{password}",
        "-e", 'NEO4J_PLUGINS=["apoc"]',
        "-e", "NEO4J_dbms_security_procedures_unrestricted=apoc.*",
        "-e", f"NEO4J_dbms_memory_heap_initial__size={heap_initial}",
        "-e", f"NEO4J_dbms_memory_heap_max__size={heap_max}",
        DEFAULT_IMAGE,
    ]

    result = subprocess.run(docker_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return False, result.stderr.strip()

    container_id = result.stdout.strip()[:12]
    return True, container_id


def stop_container() -> bool:
    """Stop and remove the Neo4j container."""
    return remove_container()


def wait_for_ready(
    uri: str = "bolt://localhost:7687",
    user: str = "neo4j",
    password: str = DEFAULT_PASSWORD,
    max_attempts: int = 30,
    delay: float = 2.0,
) -> bool:
    """Wait for Neo4j to be ready to accept connections.

    Args:
        uri: Neo4j bolt URI
        user: Neo4j username
        password: Neo4j password
        max_attempts: Maximum number of connection attempts
        delay: Delay between attempts in seconds

    Returns:
        True if connection successful, False otherwise
    """
    for attempt in range(max_attempts):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            driver.close()
            return True
        except Exception:
            if attempt < max_attempts - 1:
                time.sleep(delay)
    return False


def get_container_logs(tail: int = 50) -> str:
    """Get the last N lines of container logs."""
    result = subprocess.run(
        ["docker", "logs", "--tail", str(tail), CONTAINER_NAME],
        capture_output=True,
        text=True,
    )
    return result.stdout + result.stderr
