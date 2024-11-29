build:
	docker build \
	  --build-arg INSTALL_GROUPS="main" \
	  -t dagster-dags \
	  .

shell: build
	docker compose run \
	  --rm -it \
	  --entrypoint=/bin/bash \
	  dagster-dags

test: build
	docker compose run \
	  --rm -it \
	  dagster-dags

build-dev:
	docker build \
	  --build-arg INSTALL_GROUPS="main,dev" \
	  -t dagster-dags-dev \
	  .

ci-pyright:
	docker compose run \
	  --rm \
	  --entrypoint=pyright \
	  dagster_dags_dev \
	  --project /app/pyproject.toml \
	  /app

pyright: build-dev ci-pyright
