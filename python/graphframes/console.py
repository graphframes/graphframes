import logging

logger = logging.getLogger(__name__)

MISSING_TUTORIALS_EXTRA_MSG = (
    "The `graphframes` CLI requires the optional `tutorials` dependencies, "
    "which are not installed. Install them with: "
    "pip install 'graphframes-py[tutorials]'"
)


def main():
    try:
        import click

        from graphframes.tutorials import download, neo4j_cli
    except ImportError as err:
        logger.error("%s (%s)", MISSING_TUTORIALS_EXTRA_MSG, err)
        raise SystemExit(1) from err

    @click.group()
    def cli():
        """GraphFrames CLI: a collection of commands for graphframes."""
        pass

    cli.add_command(download.stackexchange)
    cli.add_command(neo4j_cli.neo4j)
    cli()


if __name__ == "__main__":
    main()
