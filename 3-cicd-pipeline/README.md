# CI/CD Pipeline for Data Engineering Workflows

A small PySpark sales-data transformation module with a full unit
test suite, wired to an **Azure DevOps CI/CD pipeline** that lints,
tests, and deploys automatically across **dev -> test -> prod**
environments based on branch (Agile / sprint-based delivery).

## Why this project
Directly demonstrates the JD's "Solid knowledge of CI/CD and Agile
methodologies" requirement with a real, working pipeline definition —
not just a description.

## Stack
- **PySpark** — transformation logic (`src/sales_transform.py`)
- **pytest** + **pytest-cov** — unit tests with coverage
- **flake8** — linting
- **Azure DevOps Pipelines** (`.azure-pipelines/azure-pipelines.yml`)
- **Azure Databricks** — deployment target (commands commented out —
  fill in your workspace/job IDs to activate)

## Run it locally
```bash
pip install -r requirements.txt
python -m pytest tests/ -v --cov=src
```

## Branching / environment strategy (matches the pipeline YAML)
| Branch | Deploys to |
|---|---|
| `develop` | Dev Databricks workspace |
| `release/*` | Test/UAT Databricks workspace |
| `main` | Production (behind a manual approval gate) |

Every push and PR to `main`/`develop`/`release/*` triggers:
1. Install dependencies
2. Lint with flake8
3. Run unit tests (`pytest`) and publish results + coverage
4. Deploy to the matching environment (only after tests pass)

## Set this up in Azure DevOps
1. Create a new Pipeline pointing at this repo, using
   `.azure-pipelines/azure-pipelines.yml` as the pipeline definition.
2. Create `dev`, `test`, and `production` Environments in Azure DevOps
   (Pipelines -> Environments) and add an approval check on `production`.
3. Replace the commented `databricks fs cp` / `databricks jobs run-now`
   lines with real Databricks CLI calls once you have a workspace and
   job IDs.

## What I'd extend next
- Add a `terraform`/Bicep stage to provision the Databricks job itself
- Add integration tests against a real (non-local) Spark cluster
- Add Slack/Teams notification on pipeline failure
