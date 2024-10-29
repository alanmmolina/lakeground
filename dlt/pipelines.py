import dlt
import typer
from sources import api

CLI = typer.Typer()


class Pipelines:
    @classmethod
    def tmdb(cls):
        """Pipeline for TMDB API."""
        return (
            dlt.pipeline(
                pipeline_name="api_tmdb",
                dataset_name="dlt",
                progress="enlighten",
                destination="duckdb",
            ),
            api.tmdb.source(),
        )


@CLI.command()
def run(name: str):
    """Run a pipeline by its class method name."""
    if hasattr(Pipelines, name):
        typer.echo(f"Running pipeline: {name}")
        pipeline, source = getattr(Pipelines, name)()
        pipeline.run(source)
    else:
        typer.echo(
            f"Pipeline '{name}' not found. "
            + f"Available pipelines: {', '.join([attr for attr in dir(Pipelines)])}"
        )


if __name__ == "__main__":
    CLI()
