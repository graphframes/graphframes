#!/usr/bin/env python

"""Download and decompress the Stack Exchange data dump from the Internet Archive."""

import os
from pathlib import Path

import click
import py7zr
import requests  # type: ignore

# Default data directory is relative to this module's location
DEFAULT_DATA_DIR = str(Path(__file__).parent / "data")


@click.command()
@click.argument("subdomain")
@click.option(
    "-f",
    "--folder",
    default=DEFAULT_DATA_DIR,
    help="Directory to store downloaded files (default: package data directory)",
)
@click.option(
    "--extract/--no-extract",
    default=True,
    help="Whether to extract the archive after download",
)
def stackexchange(subdomain: str, folder: str, extract: bool) -> None:
    """Download Stack Exchange archive for a given SUBDOMAIN.

    Example: graphframes stackexchange --folder /tmp/data stats.meta

    Note: This won't work for stackoverflow.com archives due to size.
    """
    # Create data directory if it doesn't exist
    os.makedirs(folder, exist_ok=True)

    # Construct archive URL and filename
    archive_url = f"https://archive.org/download/stackexchange/{subdomain}.stackexchange.com.7z"
    archive_path = os.path.join(folder, f"{subdomain}.stackexchange.com.7z")

    click.echo(f"Downloading archive from {archive_url}")

    try:
        # Download the file with retries
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = requests.get(archive_url, stream=True)
                response.raise_for_status()  # Raise exception for bad status codes
                break
            except (
                requests.exceptions.RequestException,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                requests.exceptions.Timeout,
            ) as e:
                retry_count += 1
                if retry_count == max_retries:
                    click.echo(
                        f"Failed to download after {max_retries} attempts: {e}",
                        err=True,
                    )
                    raise click.Abort()
                click.echo(f"Download attempt {retry_count} failed, retrying...")

        total_size = int(response.headers.get("content-length", 0))

        with click.progressbar(length=total_size, label="Downloading") as bar:  # type: ignore
            with open(archive_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))

        click.echo(f"Download complete: {archive_path}")

        # Extract if requested
        if extract:
            click.echo("Extracting archive...")
            output_dir = f"{subdomain}.stackexchange.com"
            with py7zr.SevenZipFile(archive_path, mode="r") as z:
                z.extractall(path=os.path.join(folder, output_dir))
            click.echo(f"Extraction complete: {output_dir}")

    except requests.exceptions.RequestException as e:
        click.echo(f"Error downloading archive: {e}", err=True)
        raise click.Abort()
    except py7zr.Bad7zFile as e:
        click.echo(f"Error extracting archive: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    stackexchange()
