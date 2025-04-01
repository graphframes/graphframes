import click

from graphframes.tutorials import download


@click.group()
def cli():
    """GraphFrames CLI: a collection of commands for graphframes."""
    pass


cli.add_command(download.stackexchange)


def main():
    cli()


if __name__ == "__main__":
    main()
