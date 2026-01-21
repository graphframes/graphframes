import click

from graphframes.tutorials import download
from graphframes.tutorials.neo4j import neo4j


@click.group()
def cli():
    """GraphFrames CLI: a collection of commands for graphframes."""
    pass


cli.add_command(download.stackexchange)
cli.add_command(neo4j)


def main():
    cli()


if __name__ == "__main__":
    main()
