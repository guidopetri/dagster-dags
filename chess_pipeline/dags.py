"""
DAG declarations for lichess ETL.
"""

import itertools
import os
from collections.abc import Iterator

import pandas as pd
from dagster import (
    AssetsDefinition,
    Config,
    DefaultSensorStatus,
    Definitions,
    RunConfig,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,  # type: ignore
    get_dagster_logger,
    make_email_on_run_failure_sensor,
    schedule,  # type: ignore
)

from utils.dagster import (
    AssetSpec,
    LoaderAssetSpec,
    make_asset,
    make_data_loader,
)


class DagRunConfig(Config):
    player: str
    perf_type: str
    data_date: str
    local_stockfish: bool = True


all_assets_job = define_asset_job(name='all_assets_job')


def get_asset_command(spec: AssetSpec, config: DagRunConfig) -> list[str]:
    return ['/app/docker_entrypoint.py',
            '--step',
            f'{spec.step}',
            '--player',
            f'{config.player}',
            '--perf_type',
            f'{config.perf_type}',
            '--data_date',
            f'{config.data_date}',
            '--local_stockfish' if config.local_stockfish else '',
            ]


def get_output(spec: AssetSpec, config: DagRunConfig) -> pd.DataFrame | bool:
    if spec.output is None:
        return True
    prefix = f'{config.data_date}_{config.player}_{config.perf_type}'
    df = pd.read_parquet(f'/io/{prefix}_{spec.output}.parquet')
    return df


data_specs: list[AssetSpec] = [
    AssetSpec(name='fetch_json',
              deps=[],
              step='fetch_json',
              output='raw_json',
              ),
    AssetSpec(name='fetch_pgn',
              deps=['fetch_json'],
              step='fetch_pgn',
              output='raw_pgn',
              ),
    AssetSpec(name='clean_df',
              deps=['fetch_json', 'fetch_pgn'],
              step='clean_df',
              output='cleaned_df',
              ),
    AssetSpec(name='get_evals',
              deps=['clean_df'],
              step='get_evals',
              output='evals',
              ),
    AssetSpec(name='explode_moves',
              deps=['clean_df'],
              step='explode_moves',
              output='exploded_moves',
              ),
    AssetSpec(name='explode_clocks',
              deps=['clean_df'],
              step='explode_clocks',
              output='exploded_clocks',
              ),
    AssetSpec(name='explode_positions',
              deps=['clean_df'],
              step='explode_positions',
              output='exploded_positions',
              ),
    AssetSpec(name='explode_materials',
              deps=['clean_df'],
              step='explode_materials',
              output='exploded_materials',
              ),
    AssetSpec(name='get_game_infos',
              deps=['clean_df'],
              step='get_game_infos',
              output='game_infos',
              ),
    AssetSpec(name='get_win_probs',
              deps=['get_evals',
                    'explode_positions',
                    'explode_clocks',
                    'get_game_infos',
                    ],
              step='get_win_probs',
              output='win_probabilities',
              ),
    ]

data_assets = [make_asset(spec=spec,
                          config_type=DagRunConfig,
                          get_command=get_asset_command,
                          get_output=get_output,
                          )
               for spec in data_specs]

loader_specs: list[LoaderAssetSpec] = [
    LoaderAssetSpec(name='load_games',
                    deps=['get_game_infos'],
                    table='chess_games',
                    ),
    LoaderAssetSpec(name='load_evals',
                    deps=['get_evals'],
                    table='position_evals',
                    ),
    LoaderAssetSpec(name='load_positions',
                    deps=['explode_positions'],
                    table='game_positions',
                    ),
    LoaderAssetSpec(name='load_materials',
                    deps=['explode_materials'],
                    table='game_materials',
                    ),
    LoaderAssetSpec(name='load_clocks',
                    deps=['explode_clocks'],
                    table='move_clocks',
                    ),
    LoaderAssetSpec(name='load_moves',
                    deps=['explode_moves'],
                    table='move_list',
                    ),
    LoaderAssetSpec(name='load_win_probs',
                    deps=['get_win_probs'],
                    table='win_probs',
                    ),
    ]
loader_assets = [make_data_loader(loader_spec=spec,
                                  config_type=DagRunConfig,
                                  get_command=get_asset_command,
                                  )
                 for spec in loader_specs]


ASSETS: list[AssetsDefinition] = data_assets + loader_assets
SPECS: list[AssetSpec | LoaderAssetSpec] = data_specs + loader_specs


@schedule(
    cron_schedule='* * * * *' if os.getenv('TESTING') else '0 1 * * *',
    job=all_assets_job,
    execution_timezone='America/Chicago',
)
def lichess_etl_schedule(context: ScheduleEvaluationContext,
                         ) -> Iterator[RunRequest]:
    date: str = context.scheduled_execution_time.strftime('%Y-%m-%d')

    players: list[str] = (['athena-pallada']
                          if os.getenv('TESTING')
                          else ['Grahtbo', 'siddhartha13'])
    perf_types: list[str] = (['blitz']
                             if os.getenv('TESTING')
                             else ['bullet', 'blitz'])

    for player, perf_type in itertools.product(players, perf_types):
        config: dict[str, str | bool] = {'player': player,
                                         'perf_type': perf_type,
                                         'data_date': date,
                                         'local_stockfish': True,
                                         }
        get_dagster_logger().info(f'Requesting run for {config=}')
        cfg: DagRunConfig = DagRunConfig(**config)  # type: ignore
        run_config: RunConfig = RunConfig(ops={spec.name: cfg
                                               for spec in SPECS})

        yield RunRequest(run_key=f'{date}_{player}_{perf_type}',
                         run_config=run_config,
                         )


email_on_run_failure = make_email_on_run_failure_sensor(
    email_from=os.getenv('EMAIL_SENDER', ''),
    smtp_user='apikey',
    smtp_host='smtp.sendgrid.net',
    email_password=os.getenv('EMAIL_SENDGRID_API_KEY', ''),
    email_to=[os.getenv('EMAIL_FAILURE_TARGET', '')],
    default_status=DefaultSensorStatus.RUNNING,
)

# todo: can i use docker_executor here?
defs = Definitions(assets=ASSETS,
                   jobs=[all_assets_job],
                   schedules=[lichess_etl_schedule],
                   sensors=[email_on_run_failure],
                   )
