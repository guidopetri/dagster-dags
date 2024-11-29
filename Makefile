build:
	docker build \
	  --build-arg INSTALL_GROUPS="main" \
	  -t dags \
	  .

shell: build
	docker compose run \
	  --rm -it \
	  --entrypoint=/bin/bash \
	  dags

build-dev:
	docker build \
	  --build-arg INSTALL_GROUPS="main,dev" \
	  -t dags-dev \
	  .

ci-pyright:
	docker compose run \
	  --rm \
	  --entrypoint=pyright \
	  dags_dev \
	  --project /app/pyproject.toml \
	  /app

pyright: build-dev ci-pyright
