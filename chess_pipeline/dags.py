
import pandas as pd
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


@asset(deps=[run_docker_hello_world])
def run_docker_json_df(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')
    volumes_to_mount = {'/mnt/dagster_io/': {'bind': '/io/', 'mode': 'rw'}}

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command='/app/docker_entrypoint.py',
                             container_kwargs={'volumes': volumes_to_mount},
                             )
    return pd.read_pickle('/mnt/dagster_io/2024_12_01_test_df.pckl')


defs = Definitions(
    assets=[run_docker_hello_world, run_docker_json_df],
    jobs=[all_assets_job],
    schedules=[],
)
