#!/usr/bin/env python

"""Download and decompress the Stack Exchange data dump from the Internet Archive."""

import os

import click
import py7zr
import requests  # type: ignore


@click.command()
@click.argument("subdomain")
@click.option(
    "--data-dir",
    default="python/graphframes/tutorials/data",
    help="Directory to store downloaded files",
)
@click.option(
    "--extract/--no-extract", default=True, help="Whether to extract the archive after download"
)
def stackexchange(subdomain: str, data_dir: str, extract: bool) -> None:
    """Download Stack Exchange archive for a given SUBDOMAIN.

    Example: python/graphframes/tutorials/download.py stats.meta

    Note: This won't work for stackoverflow.com archives due to size.
    """
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Construct archive URL and filename
    archive_url = f"https://archive.org/download/stackexchange/{subdomain}.stackexchange.com.7z"
    archive_path = os.path.join(data_dir, f"{subdomain}.stackexchange.com.7z")

    click.echo(f"Downloading archive from {archive_url}")

    try:
        # Download the file
        response = requests.get(archive_url, stream=True)
        response.raise_for_status()  # Raise exception for bad status codes

        total_size = int(response.headers.get("content-length", 0))

        with click.progressbar(length=total_size, label="Downloading") as bar:
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
                z.extractall(path=os.path.join(data_dir, output_dir))
            click.echo(f"Extraction complete: {output_dir}")

    except requests.exceptions.RequestException as e:
        click.echo(f"Error downloading archive: {e}", err=True)
        raise click.Abort()
    except py7zr.Bad7zFile as e:
        click.echo(f"Error extracting archive: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    stackexchange()
