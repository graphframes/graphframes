import click


@click.group()
def cli():
    """GraphFrames CLI: a collection of commands for graphframes."""
    pass


def main():
    from graphframes.tutorials import download

    cli.add_command(download.stackexchange)
    cli()


if __name__ == "__main__":
    main()
