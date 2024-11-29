# Dagster DAGs

This is a simple extension of the standard Dagster docker image that contains various projects' DAGs within the image itself.

## Adding a new project

To add a new project, create a new repo-level folder with a `dags.py` file within it and add it to the `workspace.yaml`:

```yaml
load_from:
  - python_module: new_project.dags
```

Additionally, copy the folder into the docker image in `Dockerfile`:

```Dockerfile
COPY new_project/ /app/new_project/
```
