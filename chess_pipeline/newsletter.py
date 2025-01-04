"""
DAG declarations for lichess activity newsletter.
"""

import itertools
import os
from collections.abc import Iterator

from dagster import (
    AssetsDefinition,
    Config,
    Definitions,
    EnvVar,
    RunConfig,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,  # type: ignore
    get_dagster_logger,
    schedule,  # type: ignore
)

from utils.dagster import (
    AssetSpec,
    make_asset,
)


class NewsletterDagRunConfig(Config):
    player: str
    category: str
    receiver: str


def get_asset_command(spec: AssetSpec,
                      config: NewsletterDagRunConfig,
                      ) -> list[str]:
    return ['/app/newsletter_entrypoint.py',
            '--step',
            f'{spec.step}',
            '--player',
            f'{config.player}',
            '--category',
            f'{config.category}',
            '--receiver',
            f'{config.receiver}',
            ]


newsletter_job = define_asset_job(name='newsletter_job')

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

data_assets = [make_asset(spec=spec,
                          config_type=NewsletterDagRunConfig,
                          get_command=get_asset_command,
                          )
               for spec in data_specs]


ASSETS: list[AssetsDefinition] = data_assets
SPECS: list[AssetSpec] = data_specs


@schedule(
    cron_schedule='* * * * *' if os.getenv('TESTING') else '0 2 * * 0',
    job=newsletter_job,
    execution_timezone='America/Chicago',
    # TODO:
    # tags_fn=,
    # description=,
    # default_status=,
    # metadata=,
)
def newsletter_schedule(context: ScheduleEvaluationContext,
                        ) -> Iterator[RunRequest]:
    date: str = context.scheduled_execution_time.strftime('%Y-%m-%d')

    players: list[str] = ['Grahtbo', 'siddhartha13']
    categories: list[str] = ['blitz']

    receiver: str | None = EnvVar('NEWSLETTER_TARGET').get_value()
    if receiver is None:
        raise ValueError('Missing newsletter target email. Please set the '
                         'NEWSLETTER_TARGET environment variable.')

    for player, category in itertools.product(players, categories):
        config: dict[str, str] = {'player': player,
                                  'category': category,
                                  'receiver': receiver,
                                  }
        get_dagster_logger().info(f'Requesting run for {config=}')
        cfg: NewsletterDagRunConfig = NewsletterDagRunConfig(**config)
        run_config: RunConfig = RunConfig(ops={spec.name: cfg
                                               for spec in SPECS})

        yield RunRequest(run_key=f'{date}_{player}_{category}',
                         run_config=run_config,
                         )


defs: Definitions = Definitions(assets=ASSETS,
                                jobs=[newsletter_job],
                                schedules=[newsletter_schedule],
                                )
