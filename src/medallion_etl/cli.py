import uuid

import typer

from medallion_etl.observability import logger
from medallion_etl.pipelines import bronze as bronze_pipe
from medallion_etl.pipelines import gold as gold_pipe
from medallion_etl.pipelines import silver as silver_pipe

app = typer.Typer(no_args_is_help=True)
PIPELINE = "medallion"


@app.command()
def bronze() -> None:
    with logger.execution(pipeline=PIPELINE):
        print(bronze_pipe.ingest_orders())


@app.command()
def silver() -> None:
    with logger.execution(pipeline=PIPELINE):
        print(silver_pipe.build_orders())


@app.command()
def gold() -> None:
    with logger.execution(pipeline=PIPELINE):
        print(gold_pipe.build_orders_daily_pipeline())


@app.command(name="all")
def run_all() -> None:
    """Run bronze, silver, gold sequentially with a shared run_id."""
    run_id = uuid.uuid4().hex[:8]
    with logger.execution(pipeline=PIPELINE):
        print(f"== run_id={run_id} ==")
        print("\n[bronze]")
        print(bronze_pipe.ingest_orders(run_id=run_id))
        print("\n[silver]")
        print(silver_pipe.build_orders(run_id=run_id))
        print("\n[gold]")
        print(gold_pipe.build_orders_daily_pipeline(run_id=run_id))
        print("\nDone.")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
