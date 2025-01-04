"""
DAG declarations for lichess activity newsletter.
"""

import itertools
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Backoff,
    Config,
    Definitions,
    EnvVar,
    Jitter,
    RetryPolicy,
    RunConfig,
    RunRequest,
    asset,  # type: ignore
    define_asset_job,  # type: ignore
    get_dagster_logger,
    schedule,
)
from dagster_docker import execute_docker_container

Asset = Callable[[AssetExecutionContext], pd.DataFrame | bool]


@dataclass
class AssetSpec:
    name: str
    deps: list[Asset]
    step: str
    output: str | None


class NewsletterDagRunConfig(Config):
    player: str
    category: str
    receiver: str


newsletter_job = define_asset_job(name='newsletter_job')


env_vars = {'DAGSTER_IO_DIR': '/io/'}

volumes_to_mount = {'/mnt/dagster_io/':
                    {'bind': env_vars['DAGSTER_IO_DIR'],
                     'mode': 'rw',
                     },
                    }


def make_asset(spec: AssetSpec) -> Asset:
    # TODO: metadata/tags
    @asset(name=spec.name,
           deps=spec.deps,
           code_version='1',
           retry_policy=RetryPolicy(max_retries=3,
                                    delay=0.2,
                                    backoff=Backoff.EXPONENTIAL,
                                    jitter=Jitter.PLUS_MINUS,
                                    ),
           )
    def asset_fn(context: AssetExecutionContext,
                 config: NewsletterDagRunConfig,
                 ):  # -> pd.DataFrame | bool:  # TODO add typing
        context.log.info(f'My run ID is {context.run.run_id}')
        context.log.info(f'{config=}')
        execute_docker_container(context=context,  # type: ignore
                                 image='chess-pipeline',
                                 entrypoint='python',
                                 command=['/app/newsletter_entrypoint.py',
                                          '--step',
                                          f'{spec.step}',
                                          '--player',
                                          f'{config.player}',
                                          '--category',
                                          f'{config.category}',
                                          '--receiver',
                                          f'{config.receiver}',
                                          ],
                                 networks=['mainnetwork'],
                                 # dagster expects env vars like NAME=value
                                 env_vars=[f'{k}={v}'
                                           for k, v in env_vars.items()],
                                 container_kwargs={'volumes': volumes_to_mount,
                                                   'auto_remove': True,
                                                   },
                                 )
    return asset_fn


data_specs: list[AssetSpec] = [
    AssetSpec(name='get_data',
              deps=[],
              step='get_data',
              output='',
              ),
    AssetSpec(name='win_ratio_by_color',
              deps=['get_data'],
              step='win_ratio_by_color',
              output='',
              ),
    AssetSpec(name='elo_by_weekday',
              deps=['get_data'],
              step='elo_by_weekday',
              output='',
              ),
    AssetSpec(name='create_email',
              deps=['win_ratio_by_color', 'elo_by_weekday'],
              step='create_email',
              output='',
              ),
    AssetSpec(name='send_email',
              deps=['create_email'],
              step='send_email',
              output='',
              ),
    ]

data_assets = [make_asset(spec) for spec in data_specs]


ASSETS: list[Asset] = data_assets
SPECS: list[AssetSpec] = data_specs


@schedule(
    cron_schedule='*/5 * * * *',
    job=newsletter_job,
    execution_timezone='America/Chicago',
)
def newsletter_schedule(context) -> Iterator[RunRequest]:
    date = context.scheduled_execution_time.strftime('%Y-%m-%d')

    players = ['Grahtbo', 'siddhartha13']
    categories = ['blitz']

    for player, category in itertools.product(players, categories):
        config = {'player': player,
                  'category': category,
                  'receiver': EnvVar('NEWSLETTER_TARGET').get_value(),
                  }
        get_dagster_logger().info(f'Requesting run for {config=}')
        cfg = NewsletterDagRunConfig(**config)
        run_config = RunConfig(ops={spec.name: cfg for spec in SPECS})

        yield RunRequest(run_key=f'{date}_{player}_{category}',
                         run_config=run_config,
                         )


defs = Definitions(
    assets=ASSETS,
    jobs=[newsletter_job],
    schedules=[newsletter_schedule],
)
