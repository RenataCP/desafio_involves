# Desafio Involves

A local data platform running on Kubernetes that extracts Pokémon data from the
PokéAPI, processes it through a raw → curated pipeline orchestrated by Airflow,
and exposes it via a REST API.

## Architecture

PokeAPI → Airflow DAG → PostgreSQL (raw.pokeapi) → PostgreSQL (curated.pokemon) → Pokedex API


- **Orchestration:** Apache Airflow 2.10.5 with KubernetesExecutor (each task runs as a Pod)
- **Storage:** PostgreSQL — one instance serves both Airflow metadata and pipeline data
- **Pipeline:** Python DAG extracts from PokéAPI, stages raw JSON, transforms to structured table
- **API:** FastAPI application exposing curated data via Swagger UI
- **Cluster:** k3d (k3s in Docker) — lightweight local Kubernetes

## Prerequisites

| Tool    | Version  |
|---------|----------|
| Docker  | 27+      |
| k3d     | v5.x     |
| kubectl | v1.31+   |
| Helm    | v3.15+   |
| Helmfile| v0.169+  |

## Setup
### 1. Clone

```bash
git clone git@github.com:RenataCP/desafio_involves.git
cd desafio_involves
```


### 2. Fill in secrets
Copy all `.example` files and fill in the values:

```bash
cp k8s/airflow/values-secrets.yaml.example k8s/airflow/values-secrets.yaml
cp k8s/airflow/data-db-secrets.yaml.example k8s/airflow/data-db-secrets.yaml
cp k8s/postgres/values-secrets.yaml.example k8s/postgres/values-secrets.yaml
cp k8s/pokedex/db-secrets.yaml.example k8s/pokedex/db-secrets.yaml
cp k8s/pokedex/ghcr-secrets.yaml.exemple k8s/pokedex/ghcr-secrets.yaml
```

The git-sync SSH key requires a deploy key registered on this repository:

```bash
# generate key pair
ssh-keygen -t ed25519 -C "airflow-gitsync" -f k8s/airflow/gitsync-key -N ""
# register k8s/airflow/gitsync-key.pub as a Deploy Key on GitHub (read-only)
# paste the private key content into k8s/airflow/git-sync-secrets.yaml
```


### 3. Create cluster
```bash
k3d cluster create data-platform --config k8s/cluster/k3d-config.yaml
kubectl config use-context k3d-data-platform
```


### 4. Deploy PostgreSQL
```bash
helmfile apply k8s/postgres/helmfile.yaml
```

Wait for the pod to be ready:
```bash
kubectl get pods -n postgres
```


### 5. Deploy Airflow
```bash
kubectl create namespace airflow
kubectl apply -f k8s/airflow/logs-pvc.yaml
kubectl apply -f k8s/airflow/git-sync-secrets.yaml
kubectl apply -f k8s/airflow/data-db-secrets.yaml
helmfile apply -f k8s/airflow/helmfile.yaml
```

### 6. Run the pipeline
Access Airflow at http://localhost:39090 (admin credentials in values-secrets.yaml).

Activate the `pokemon_etl` DAG. It will:

1. Create database schemas and tables
2. Grant access to the API user
3. Fetch Pokémon from PokéAPI into raw.pokeapi
4. Transform and load into curated.pokemon


### 7. Deploy Pokedex API
```bash
kubectl create namespace pokedex
kubectl apply -f k8s/pokedex/ghcr-secrets.yaml
kubectl apply -f k8s/pokedex/db-secrets.yaml
kubectl apply -f k8s/pokedex/deployment.yaml
kubectl apply -f k8s/pokedex/service.yaml
```


## Services
| Service | URL |
|---------|-----|
|Airflow UI | http://localhost:39090 |
|Pokedex API (Swagger) | http://localhost:39091/docs |

## CI/CD
Three GitHub Actions workflows trigger on pull requests:
| Workflow | Path | Steps |
|----------|------|-------|
|platform.yaml | k8s/** | YAML lint, Helm template validation, secret scanning |
|dags.yaml | dags/** | 	Ruff lint, DAG import validation, owner check |
|app.yaml | app/** | 	Ruff lint, Docker build + push to GHCR |

