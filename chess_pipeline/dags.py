
from dagster import (
    AssetExecutionContext,
    Backoff,
    Config,
    Definitions,
    Jitter,
    RetryPolicy,
    asset,  # type: ignore
    define_asset_job,  # type: ignore
)
from dagster_docker import execute_docker_container


class DagRunConfig(Config):
    player: str
    perf_type: str
    date: str
    local_stockfish: bool = True


all_assets_job = define_asset_job(name='all_assets_job')


# TODO: metadata/tags
@asset(deps=[],
       code_version='1',
       retry_policy=RetryPolicy(max_retries=3,
                                delay=0.2,
                                backoff=Backoff.EXPONENTIAL,
                                jitter=Jitter.PLUS_MINUS,
                                ),
       )
def run_docker_hello_world(context: AssetExecutionContext,
                           config: DagRunConfig,
                           ) -> None:
    context.log.info(f'My run ID is {context.run.run_id}')
    context.log.info(f'{config=}')
    execute_docker_container(context=context,  # type: ignore
                             image='hello-world',
                             )
    return


defs = Definitions(
    assets=[run_docker_hello_world],
    jobs=[all_assets_job],
    schedules=[],
)
