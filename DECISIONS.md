# Technical Decisions

This document records the key technical decisions made during the project.

---

## Infrastructure

### k3d over Minikube or Kind
Chosen for lower resource overhead and native support for multi-node clusters
across machines on the same network — relevant due hardware limitations in a local
environment. k3d runs k3s inside Docker containers, making cluster creation and 
deletion fast and isolated from the host machine.

### No docker-compose.yml
The original challenge specification included a `docker-compose.yml` for local
stack setup. This was replaced entirely by Kubernetes (k3d), which provides a
closer approximation to production environments, native support for the
KubernetesExecutor, and better resource isolation between services.

### Single-node cluster (agents: 0)
In production, additional agent nodes would distribute workload. For this
demonstration, running with zero agents keeps resource usage within the limits
of a development machine. The coordinator node handles all scheduling.

### Non-standard port range
Ports `39090` (Airflow) and `39091` (Pokedex API) were chosen to avoid conflicts
with other local projects running on common ports (8080, 8000, etc.).
In production, services would be exposed via ingress controllers or load balancers,
not NodePort.

### k8s folder structure (one folder per namespace)
Each service has its own folder under `k8s/` containing all related manifests,
helmfiles, and values files. This introduces some duplication but provides clear
isolation and makes it straightforward to deploy or tear down a single component
without affecting others.

### Secrets as local files (gitignored)
In production, sensitive data would be managed via dedicated secrets management
solution integrated with Kubernetes. For this demonstration, secrets are kept
in local YAML files excluded from version control via `.gitignore`. Each secret
file has a corresponding `.example` file committed to the repository documenting
the required fields.

### Branch protection not enforced
In production, direct commits to `main` would be blocked, requiring all changes
to go through a pull request with passing CI checks. This was intentionally left
unconfigured here to allow flexibility across multiple working contexts within a
single repository.

---

## Orchestration

### Apache Airflow with KubernetesExecutor
KubernetesExecutor was chosen over LocalExecutor or CeleryExecutor because it
runs each task as an isolated Kubernetes Pod. This aligns with the platform's
design principle of isolating workloads, avoids the need for a Redis broker, and
makes resource limits per task explicit.

### External PostgreSQL (not Airflow's bundled chart)
Airflow's Helm chart includes a bundled PostgreSQL, but an external PostgreSQL
instance was deployed separately to give full control over the database lifecycle,
allow it to serve multiple purposes (Airflow metadata and pipeline data), and avoid
coupling the Airflow chart upgrade path to the database.

### Git-sync sidecar for DAG delivery
DAGs are synchronized from GitHub via a git-sync sidecar container rather than
being baked into a custom Airflow image. This allows DAG changes to be deployed
without rebuilding or redeploying the Airflow platform. Trade-off: the sidecar
has no path filtering — it syncs the entire repository. For this project the
DAG folder path is configured in Airflow to ignore non-DAG files. In a production
environment, DAGs should live in a dedicated repository separate from infrastructure
configuration.

---

## Data Pipeline

### PostgreSQL as both metadata and data store
A single PostgreSQL instance serves Airflow's metadata database and the pipeline's
raw and curated data layers. This simplifies the infrastructure for a demonstration
project. In production, these would be separate instances with independent backup
and scaling policies.

### Raw → curated layer design
Pipeline data is organized into two schemas within the `data` database:
- `raw`: append-only tables faithful to the source and never modified after ingestion.
- `curated`: upserted table with typed columns ready for consumption. 

This mimics the medallion architecture pattern without requiring object storage
or an open table format at this scale.

### PokéAPI as data source
Chosen for being publicly available without authentication or rate limits that
would disrupt a live demonstration. 

### Access management via DAG task
Database grants for application users are applied by a dedicated task 
(`setup_app_access`) within the `pokemon_etl` DAG, running after table creation.
This makes access configuration reproducible and version-controlled alongside the
pipeline. This solution is NOT recomendated for poduction. A similar way, access
management could be handled by a dedicated platform DAG owned by the infrastructure
team, potentially integrated with an identity management tool and including credential
lifecycle policies.

---

## Application

### FastAPI for the Pokedex API
FastAPI was chosen for its built-in Swagger UI (available at `/docs`), automatic
request validation, and minimal boilerplate. The API serves as the consumption
layer over the curated data, demonstrating that the pipeline output is queryable
by an external application.

### GHCR for container registry
GitHub Container Registry was chosen as the image registry because it integrates
directly with GitHub Actions using the existing `GITHUB_TOKEN` without additional
credential configuration. Images are private by default and linked to the
repository.

---

## CI/CD

### Three separate workflows by concern
- `platform.yaml`: validates Kubernetes and Helm configurations (`k8s/**`)
- `dags.yaml`: validates DAG code quality and structure (`dags/**`)
- `app.yaml`: validates and packages the API application (`app/**`)

Each workflow triggers only on changes to its relevant path, avoiding unnecessary
runs.

### DAG validation tests
Two tests were implemented to demonstrate the possibilities:
1. `test_dags_load_without_import_errors`: catches broken DAGs before they reach
   the Airflow
2. `test_all_dags_have_owners`: enforces a metadata standard across all DAGs

These tests establish the pattern for expanding governance enforcement over time —
additional checks (schedule validation, tag requirements, timeout enforcement) can
be added following the same structure.
