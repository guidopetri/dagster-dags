"""
Utilities for Dagster DAGs.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Backoff,
    Config,
    Jitter,
    RetryPolicy,
    asset,  # type: ignore
)
from dagster_docker import execute_docker_container

Asset = Callable[[AssetExecutionContext], pd.DataFrame | bool]


@dataclass
class GenericAssetSpec:
    """
    Metaclass for asset specs.
    """

    name: str
    deps: list[str]


@dataclass
class AssetSpec(GenericAssetSpec):
    step: str
    output: str | None


@dataclass
class LoaderAssetSpec(GenericAssetSpec):
    table: str


env_vars = {'DAGSTER_IO_DIR': '/io/'}

volumes_to_mount = {'/mnt/dagster_io/':
                    {'bind': env_vars['DAGSTER_IO_DIR'],
                     'mode': 'rw',
                     },
                    '/home/users/loki/data/chess_pipeline/config/':
                    {'bind': '/config',
                     'mode': 'rw',
                     },
                    }


class SpecHandler(Protocol):
    """
    Protocol for a function that handles a subclass of GenericAssetSpec and a
    subclass of Config.

    Callable.
    """

    # TODO: remove Any
    def __call__(self,
                 spec: GenericAssetSpec,
                 config: Config,
                 ) -> Any:
        ...


def _no_output(spec: GenericAssetSpec, config: Config) -> None:
    """
    Dummy function to signify that an asset does not return any output.
    """
    return None


def make_asset(spec: GenericAssetSpec,
               config_type: type[Config],
               get_command: SpecHandler,
               get_output: SpecHandler = _no_output,
               ) -> AssetsDefinition:
    # TODO: metadata/tags
    # TODO: add partition key definition
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
                 config: config_type,
                 ) -> Any:  # TODO add typing
        context.log.info(f'My run ID is {context.run.run_id}')
        context.log.info(f'{config=}')
        execute_docker_container(context=context,  # type: ignore
                                 image='chess-pipeline',
                                 entrypoint='python',
                                 command=get_command(spec=spec, config=config),
                                 networks=['main-network'],
                                 # dagster expects env vars like NAME=value
                                 env_vars=[f'{k}={v}'
                                           for k, v in env_vars.items()],
                                 container_kwargs={'volumes': volumes_to_mount,
                                                   'auto_remove': True,
                                                   },
                                 )
        return get_output(spec=spec, config=config)
    return asset_fn


def make_data_loader(loader_spec: LoaderAssetSpec,
                     config_type: type[Config],
                     get_command: SpecHandler,
                     ) -> AssetsDefinition:
    spec: AssetSpec = AssetSpec(name=loader_spec.name,
                                deps=loader_spec.deps,
                                step=f'load_{loader_spec.table}',
                                output=None,
                                )
    return make_asset(spec=spec,
                      config_type=config_type,
                      get_command=get_command,
                      get_output=_no_output,
                      )
