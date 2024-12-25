
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
    data_date: str
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


@asset
def run_docker_json_df(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'fetch_json',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/raw_json.parquet')
    return df


@asset(deps=[run_docker_json_df])
def run_docker_pgn_df(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'fetch_pgn',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/raw_pgn.parquet')
    return df


@asset(deps=[run_docker_json_df, run_docker_pgn_df])
def run_docker_clean_df(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'clean_df',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/cleaned_df.parquet')
    return df


@asset(deps=[run_docker_clean_df])
def run_docker_get_evals(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'get_evals',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/evals.parquet')
    return df


@asset(deps=[run_docker_clean_df])
def run_docker_explode_moves(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'explode_moves',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/exploded_moves.parquet')
    return df


@asset(deps=[run_docker_clean_df])
def run_docker_explode_clocks(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'explode_clocks',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/exploded_clocks.parquet')
    return df


@asset(deps=[run_docker_clean_df])
def explode_positions(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'explode_positions',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/exploded_positions.parquet')
    return df


@asset(deps=[run_docker_clean_df])
def explode_materials(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'explode_materials',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/exploded_materials.parquet')
    return df


@asset(deps=[run_docker_clean_df])
def run_docker_get_game_infos(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'get_game_infos',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/game_infos.parquet')
    return df


@asset(deps=[run_docker_get_evals,
             explode_positions,
             run_docker_explode_clocks,
             run_docker_get_game_infos,
             ])
def run_docker_get_win_probs(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'get_win_probs',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    df = pd.read_parquet('/mnt/dagster_io/win_probabilities.parquet')
    return df


@asset(deps=[run_docker_get_game_infos])
def run_docker_load_chess_games(context: AssetExecutionContext) -> bool:
    context.log.info(f'My run ID is {context.run.run_id}')

    env_vars = {'DAGSTER_IO_DIR': '/io/'}

    volumes_to_mount = {'/mnt/dagster_io/':
                        {'bind': env_vars['DAGSTER_IO_DIR'],
                         'mode': 'rw',
                         },
                        }

    execute_docker_container(context=context,  # type: ignore
                             image='chess-pipeline',
                             entrypoint='python',
                             command=['/app/docker_entrypoint.py',
                                      '--step',
                                      'load_chess_games',
                                      ],
                             networks=['mainnetwork'],
                             # dagster expects env vars in NAME=value format
                             env_vars=[f'{k}={v}'
                                       for k, v in env_vars.items()],
                             container_kwargs={'volumes': volumes_to_mount,
                                               'auto_remove': True,
                                               },
                             )
    return True


defs = Definitions(
    assets=[run_docker_hello_world,
            run_docker_json_df,
            run_docker_pgn_df,
            run_docker_clean_df,
            run_docker_get_evals,
            run_docker_explode_moves,
            run_docker_explode_clocks,
            explode_positions,
            explode_materials,
            run_docker_get_game_infos,
            run_docker_get_win_probs,
            run_docker_load_chess_games,
            ],
    jobs=[all_assets_job],
    schedules=[],
)
