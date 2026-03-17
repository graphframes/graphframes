import click


@click.group()
def cli():
    """GraphFrames CLI: a collection of commands for graphframes."""
    pass


def main():
    # Lazy-import tutorials.download to avoid requiring py7zr/requests
    # at import time — those are only in the 'tutorials' optional extra.
    from graphframes.tutorials import download

    cli.add_command(download.stackexchange)
    cli()


if __name__ == "__main__":
    main()
