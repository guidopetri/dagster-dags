"""
DAG declarations for lichess ETL.
"""

from dataclasses import dataclass
from typing import Callable

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

Asset = Callable[[AssetExecutionContext], pd.DataFrame | bool]


@dataclass
class AssetSpec:
    name: str
    deps: list[Asset]
    step: str
    output: str | None


@dataclass
class LoaderAssetSpec:
    name: str
    deps: list[Asset]
    table: str


class DagRunConfig(Config):
    player: str
    perf_type: str
    data_date: str
    local_stockfish: bool = True


all_assets_job = define_asset_job(name='all_assets_job')


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
                 config: DagRunConfig,
                 ):  # -> pd.DataFrame | bool:  # TODO add typing
        context.log.info(f'My run ID is {context.run.run_id}')
        context.log.info(f'{config=}')
        execute_docker_container(context=context,  # type: ignore
                                 image='chess-pipeline',
                                 entrypoint='python',
                                 command=['/app/docker_entrypoint.py',
                                          '--step',
                                          f'{spec.step}',
                                          # TODO use config
                                          ],
                                 networks=['mainnetwork'],
                                 # dagster expects env vars like NAME=value
                                 env_vars=[f'{k}={v}'
                                           for k, v in env_vars.items()],
                                 container_kwargs={'volumes': volumes_to_mount,
                                                   'auto_remove': True,
                                                   },
                                 )
        if spec.output is None:
            return True
        df = pd.read_parquet(f'/mnt/dagster_io/{spec.output}.parquet')
        return df
    return asset_fn


def make_data_loader(loader_spec: LoaderAssetSpec) -> Asset:
    spec = AssetSpec(name=loader_spec.name,
                     deps=loader_spec.deps,
                     step=f'load_{loader_spec.table}',
                     output=None,
                     )
    return make_asset(spec)


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

data_assets = [make_asset(spec) for spec in data_specs]

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
loader_assets = [make_data_loader(spec) for spec in loader_specs]


ASSETS: list[Asset] = data_assets + loader_assets

defs = Definitions(
    assets=ASSETS,
    jobs=[all_assets_job],
    schedules=[],
)
