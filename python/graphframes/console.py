import click


@click.group()
def cli():
    """GraphFrames CLI: a collection of commands for graphframes."""
    pass


def main():
    from graphframes.tutorials import download, neo4j_cli

    cli.add_command(download.stackexchange)
    cli.add_command(neo4j_cli.neo4j)
    cli()


if __name__ == "__main__":
    main()
