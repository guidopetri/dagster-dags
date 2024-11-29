build:
	docker build \
	  --build-arg INSTALL_GROUPS="main" \
	  -t project \
	  .

shell: build
	docker compose run \
	  --rm -it \
	  --entrypoint=/bin/bash \
	  project

build-dev:
	docker build \
	  --build-arg INSTALL_GROUPS="main,dev" \
	  -t project-dev \
	  .

ci-pyright:
	docker compose run \
	  --rm \
	  --entrypoint=pyright \
	  project_dev \
	  --project /app/pyproject.toml \
	  /app

pyright: build-dev ci-pyright
