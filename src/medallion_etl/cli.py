import typer

from medallion_etl.pipelines import bronze as bronze_pipe
from medallion_etl.pipelines import gold as gold_pipe
from medallion_etl.pipelines import silver as silver_pipe

app = typer.Typer(no_args_is_help=True)


@app.command()
def bronze() -> None:
    print(bronze_pipe.ingest_orders())


@app.command()
def silver() -> None:
    print(silver_pipe.build_orders())


@app.command()
def gold() -> None:
    print(gold_pipe.build_orders_daily_pipeline())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
