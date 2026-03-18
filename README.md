# FiftyOne Sync Service

Backend service to sync data between Tator and Voxel51 for quickly editing localizations
and downstream model iteration.

See https://docs.mbari.org/internal/ai/videos/voxel51demo.gif for demo of the community Voxel51 tool.

Supports both [Voxel51 Community](https://github.com/voxel51/fiftyone) and Voxel51 Enterprise sync.  Syncing is done by version through a simple applet.

### Applet for Tator dashboard to sync to Voxel51. When syncing is done, a clickable link is provided.
![template_applet.png](docs/imgs/template_applet.png)
### Example Voxel51 embedding/grid view. Samples can be refined by lassoing an embedding cluster, or by filtering onmetadata (e.g. depth, label, confidence):
![embedding_grid.png](docs/imgs/embedding_grid.png)
### Example Voxel51 similarity search view
![sim_search.png](docs/imgs/sim_search.png)
### FastAPI
![fastapi.png](docs/imgs/fastapi.png)

## Architecture

```mermaid
flowchart TD
    subgraph Browser["Browser (User)    "]
        UI["Tator Dashboard\n(Hosted Template Applet)"]
        FOTab["FiftyOne App\n(New Browser Tab)"]
    end

    subgraph TatorBackend["Tator Backend (Docker)"]
        Gunicorn["Tator / Gunicorn"]
    end

    subgraph FiftyOneSync["fiftyone-sync Service (port 8001)"]
        API["FastAPI\nmain.py"]
        Launcher["Launcher Template\nlauncher_template.py"]
        EmbedSvc["Embedding Service\nembedding_service.py"]
        DBMgr["Database Manager\ndatabase_manager.py"]
        SyncQueue["Sync Queue\nsync_queue.py"]
        SyncWorker["Sync Worker\nsync_worker.py"]
        SyncLogic["Sync Logic\nsync.py"]
    end

    subgraph ExternalServices["External Services"]
        Tator["Tator REST API"]
        FastVSS["Fast-VSS\n(Embedding Service)\nport 8000"]
        Redis["Redis\n(Job Queue)"]
        MongoDB["MongoDB\n(FiftyOne DB)"]
        FOApp["FiftyOne App\n(port 515x per project)"]
        S3["AWS S3\n(optional crop storage)"]
    end

    %% Tator fetches the launcher template
    Gunicorn -->|"GET /render"| API
    API --> Launcher

    %% User interactions
    UI -->|"GET /launch\nGET /versions\nPOST /sync\nPOST /sync-to-tator"| API
    UI -->|"Open FiftyOne"| FOTab
    FOTab -->|"HTTP port 515x"| FOApp

    %% Sync flow
    API -->|"enqueue job"| SyncQueue
    SyncQueue -->|"job"| Redis
    Redis -->|"dequeue job"| SyncWorker
    SyncWorker --> SyncLogic
    SyncLogic -->|"fetch media\n& localizations"| Tator
    SyncLogic -->|"write dataset"| MongoDB
    SyncLogic -->|"launch app"| FOApp
    SyncLogic -->|"upload crops (optional)"| S3

    %% Embeddings flow
    API -->|"POST /embed\nGET /embed/{uuid}"| EmbedSvc
    EmbedSvc -->|"POST /embeddings/{project}/\nWS /ws/predict/job/{id}/{project}"| FastVSS

    %% Database / config
    API --> DBMgr
    DBMgr -->|"URI / port lookup"| MongoDB

    %% Status polling
    UI -->|"GET /sync/status/{job_id}"| API
    API -->|"poll job status"| Redis
```

## Features

- **Embedding API**: Delegates to Fast-VSS (`http://localhost:8000/embeddings/{project}/`)
  - `POST /embed` - Submit images (multipart/form-data) + project, returns UUID
  - `GET /embed/{uuid}` - Poll for results (job status from Fast-VSS via WebSocket `/ws/predict/job/{job_id}/{project}`)
  - Set `FASTVSS_API_URL` env var to override Fast-VSS base URL

- **Port isolation**: One FiftyOne App instance per Tator project (one port per project)
  - Port = 5151 + (project_id - 1)

- **MongoDB database isolation**: A single MongoDB is launched by default (see `containers/fiftyone-sync`). **Project 1** uses database `fiftyone_project_1` on that instance; other projects use `fiftyone_project_2`, etc.
  - Default: database name = `{FIFTYONE_DATABASE_DEFAULT}_{project_id}` (env `FIFTYONE_DATABASE_DEFAULT` defaults to `fiftyone_project`)
  - Override (all projects): set env `FIFTYONE_DATABASE_NAME` to use a single shared database
  - Override (per request): pass optional query param `database_name` on `GET /launch` and `POST /sync`

- **Launcher**: HostedTemplate integration
  - `GET /message` - Minimal template with single `{{ message }}` (for simple Hosted Template testing)
  - `GET /render` - Launcher template with **Open FiftyOne** (opens app in new window) and **Sync from Tator** (tparams: project, iframe_host, base_port, sync_service_url, api_url; user enters API token in the applet and clicks "Verify Token" to enable sync)
  - `GET /launch` - Allocate port, return FiftyOne App URL
  - `POST /sync` - Enqueue Tator-to-FiftyOne sync (requires Redis): fetch media + localizations, crop, build FiftyOne dataset, launch app. Returns `job_id` immediately; poll `GET /sync/status/{job_id}` for completion.
  - `GET /sync/status/{job_id}` - Poll status of a queued sync job.
  - `POST /sync-to-tator` - Push FiftyOne dataset edits (labels, confidence) back to Tator localizations
  - `GET /versions` - Return Tator versions for a project (query params: `project_id`, `api_url`; token via `Authorization: Token <token>` header only)
  - FiftyOne opens in a **new browser tab** (not in an iframe). Set **`iframe_host`** to the host where the FiftyOne app runs so the Open FiftyOne URL is correct (e.g. same host as Tator or `localhost`).

- **Sync queue (Redis required)**: Sync runs in a background worker. Set `REDIS_HOST=redis` (or your Redis host) for the API; run the sync worker with the same Redis and env (MongoDB, etc.): `python -m src.app.sync_worker`. The launcher enqueues sync on click and polls until the worker finishes. Redis env: `REDIS_HOST`, `REDIS_PORT` (default `6379`), `REDIS_PASSWORD`, `REDIS_USE_SSL`; or a single `REDIS_URL` (e.g. `redis://host:6380/0`).

## Run (Docker)

The service is intended to be run via the compose stack, which starts MongoDB(community only) and the API:

```bash
# From repo root
docker compose -f containers/fiftyone-sync/compose.yaml up -d
```

API: http://localhost:8001. Optional env: copy `containers/fiftyone-sync/.env.example` to `containers/fiftyone-sync/.env` to set `FASTVSS_API_URL`, `REDIS_HOST`, etc.

## Development

For local iteration (no Docker for the API):

```bash
cd services/fiftyone-sync
export FIFTYONE_DATABASE_URI=mongodb://localhost:27017
uvicorn src.app.main:app --host 0.0.0.0 --port 8001
```

Use a venv and install deps first: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`. Start MongoDB separately (e.g. `docker compose -f containers/fiftyone-sync/compose.yaml up -d mongo`).

## Setup (for development)

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

```

## AWS S3 (crop image upload) Enterprise ONLY

You can optionally sync **crop images** (localization crops, not full images) from a Tator sync to an S3 bucket and build a FiftyOne dataset. The sync worker uploads the crops directory to S3 (layout: `s3://bucket/prefix/media_stem/elemental_id.png`), then lists the bucket with FiftyOne storage and creates a second dataset (suffix `_raw`). Parent folder in S3 is used as the sample label.

### config.yml

Per-project S3 is configured in the same YAML file used for database URIs. Set the path via **`FIFTYONE_SYNC_CONFIG_PATH`** (e.g. `config.yml` or an absolute path). Under each project key, add optional **`s3_bucket`** and **`s3_prefix`**:

```yaml
# config.yml (or the file pointed to by FIFTYONE_SYNC_CONFIG_PATH)
projects:
  "my-project-name":
    vss_project: "optional-vss-project"
    s3_bucket: "my-bucket"           # required for S3 upload
    s3_prefix: "fiftyone/raw"        # optional; S3 prefix (folder) under the bucket
    databases:
      - uri: "mongodb://localhost:27017"
        port: 5151
```

- **`s3_bucket`** (optional): S3 bucket name. When set, the applet shows S3 bucket/prefix fields (only after the user has valid credentials). Sync will upload the **crops** directory (not full images) to this bucket and build a dataset from the listed S3 objects. The bucket is created automatically if it does not exist.
- **`s3_prefix`** (optional): Key prefix (folder path) inside the bucket, e.g. `fiftyone/raw`. Omit or leave empty to use the bucket root.

Project keys must match the **Tator project name** (from the API), not the project ID.

### AWS credentials

The sync worker (and any process that runs sync) needs AWS credentials with permission to **upload** to the bucket (`s3:PutObject`, and list if applicable). Use one of:

- **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_DEFAULT_REGION` (or `AWS_REGION`). Set these in the environment of the sync worker (e.g. in Docker or your process manager).
- **IAM role**: When the worker runs on AWS (EC2, ECS, Lambda, etc.), attach an IAM role with the same S3 permissions; no env vars needed.
- **AWS CLI config**: If the worker runs in an environment where `aws configure` has been used, the default profile is used.

Upload uses the most efficient path available: **`aws s3 sync`** when the AWS CLI is installed, otherwise **boto3** (upload per file). The bucket is created if it does not exist. Ensure credentials have access before running a sync with S3 enabled.

### Applet behavior

When a project has `s3_bucket` set in config, the launcher applet shows **S3 bucket** and **S3 prefix** inputs only after the user has entered a valid token and passed **Verify Token** (and a database entry exists for the project). Values from config are pre-filled; the user can override them before clicking **Load from Tator**. Sync then uses the applet values or, if omitted, the config values.

## Testing

Sync requires Redis and a running sync worker.

### Setup

**Docker**: Use `REDIS_HOST=redis` (and network to Tator’s Redis) or run Redis and set `REDIS_HOST` in `containers/fiftyone-sync/.env`. Run the sync worker in a separate container or on the host with the same env.

**Development**:

1. Start Redis (e.g. `docker run -d --name redis -p 6379:6379 redis:7` or use Tator compose Redis).

2. Start the API with Redis:
   ```bash
   cd services/fiftyone-sync
   export FIFTYONE_DATABASE_URI=mongodb://localhost:27017
   export REDIS_HOST=localhost
   uvicorn src.app.main:app --host 0.0.0.0 --port 8001
   ```

3. Start the sync worker (another terminal, same env):
   ```bash
   cd services/fiftyone-sync
   source .venv/bin/activate
   export REDIS_HOST=localhost
   export FIFTYONE_DATABASE_URI=mongodb://localhost:27017
   python -m src.app.sync_worker
   ```

4. **Trigger sync** (e.g. from the Tator dashboard **Sync from Tator** button, or via curl):
   ```bash
   curl -X POST "http://localhost:8001/sync?project_id=1&api_url=https://your-tator.example.com&token=YOUR_TOKEN"
   ```
   Response is `{"job_id":"...", "status":"queued", "port":...}`.

5. **Poll status** (replace `JOB_ID` with the returned `job_id`):
   ```bash
   curl "http://localhost:8001/sync/status/JOB_ID"
   ```
   Keep polling until `"status":"finished"` (then `result` has `dataset_name`, `sample_count`) or `"status":"failed"` (then `error` is set).

6. In the **browser**, after clicking **Sync from Tator** you should see “Sync queued…”, then “Sync in progress…”, then “Sync done. Opening FiftyOne…” without the tab freezing.

## Hosted Template applet (recommended)

This service exposes a Jinja2 template for [Tator Hosted Templates](https://www.tator.io/docs/developer-guide/applets-and-dashboards/hosted-templates). When an applet uses it, Tator fetches the template and renders it with template parameters. The dashboard shows **Open FiftyOne** (opens the app in a new tab) and sync controls (when sync_service_url and api_url are set). Users enter their Tator API token in the applet and click **Verify Token** to verify it; the Version dropdown and **Sync from Tator** / **Sync to Tator** buttons then become enabled.

### 1. Register the Hosted Template (organization level)

1. In Tator, go to **Organizations** in the main menu and open your organization.
2. In the left sidebar, under **Hosted Templates**, click **+ Add new**.
3. Fill in:
   - **Name**: e.g. `FiftyOne Viewer`
   - **URL**: `http://<fiftyone-sync-host>:<port>/render`
     For a minimal test (message only), use `/message` instead and add a template parameter **message** (e.g. `A is for Apple`).
     Example: `http://localhost:8001/render` (use the URL where this service is running).
   - **Headers**: Leave empty unless the service requires auth.
   - **Template parameters** (optional defaults):
     - `base_port`: `5151`
     - `iframe_host`: host for the FiftyOne app URL when opening in a new tab. **Use the same host you use to open Tator.** If you open Tator at `http://134.89.17.13:8080`, set `iframe_host` to `134.89.17.13` so the app URL is `http://134.89.17.13:5181/...`. If you use `localhost`, the app will try to load from the user’s machine (connection refused when Tator is opened from another host). Do **not** use `host.docker.internal` (browsers cannot resolve it).
     - `message`: optional header text (if set, replaces the default status line)
     - `config_yaml`: optional YAML config string for FiftyOne; shown in the header and exposed as `window.FIFTYONE_CONFIG_YAML` for scripts
     - **Sync from Tator / Sync to Tator**: set `sync_service_url` and `api_url` (do **not** set a token tparam). In the applet, the user enters their Tator API token in the password field and clicks **Verify Token**; once the token is verified, the Version dropdown is filled and **Sync from Tator** / **Sync to Tator** are enabled. Optionally set **version_id** to preselect a version. The token is used only in the browser for sync and is not stored.

Click **Save**.

### 2. Register an applet using the Hosted Template (project level)

1. Open **Project Settings** for the project where you want the FiftyOne dashboard.
2. In the left sidebar, click **Applets** → **+ Add new**.
3. Set **Name** (e.g. `FiftyOne`) and **Description**.
4. Leave **HTML File** blank.
5. Under **Hosted Template**, select the template you created (e.g. `FiftyOne Viewer`).
6. Under **Template parameters**, add:
   - **name**: `project`
   - **value**: the project ID (same as this project’s ID).

   Set **`iframe_host`** to the host you use to open Tator (e.g. `134.89.17.13`). If you use `localhost` but open Tator at a different host, the iframe will show "connection refused" because the browser will try to load FiftyOne from the user’s machine, not the server.

7. Click **Save**.

### 3. Open the applet

Go to the project, then **Analytics** → **Dashboards**. Open the applet. Click **Open FiftyOne** to open the viewer in a new tab, or **Sync from Tator** first to sync data and then open the viewer.

### Recommended settings: Tator in Docker (localhost:8080), fiftyone-sync on host (port 8001)

| Setting | Value | Who uses it |
|---------|-------|-------------|
| **URL** (Hosted Template) | `http://host.docker.internal:8001/render` | Tator (in Docker) fetches the template from the host. On Docker Desktop (Mac/Windows) use `host.docker.internal`. On Linux use the host IP (e.g. `172.17.0.1:8001`) or run sync in Docker on the same network. |
| **base_port** | `5151` | Must match `database_manager.BASE_PORT`. |
| **iframe_host** | `localhost` | Host for the FiftyOne app URL when opening in a new tab. |
| **sync_service_url** | `http://localhost:8001` | Required for the "Sync from Tator" button; same machine as Tator from the user's perspective. |
| **api_url** | `http://localhost:8080` | Sync service calls Tator's API; from the host, Tator is at localhost:8080. |

There is no template parameter named **host**; the **URL** field is where Tator fetches the template from. Do **not** set a token tparam; users enter their Tator API token in the applet and click **Verify Token** to enable sync. The version list and sync requests use the token in the `Authorization` header (for `/versions`) or in the request (for sync); the token is not stored.

### Tator in Docker: "Connection refused" and "host.docker.internal server IP address not found"

The Hosted Template URL is **fetched by Tator’s backend** (gunicorn), not by the user’s browser. So the URL must be reachable from inside the container (or host) where gunicorn runs.

- **Connection refused**: Hosted Template URL is fetched by gunicorn — use e.g. `http://host.docker.internal:8001/render`. **"host.docker.internal server IP address not found"**: Set template param **`iframe_host`** to `localhost` (not `host.docker.internal`) so the Open FiftyOne URL works in the browser.
- **If Tator runs in Docker and this service runs on the host**
  - Hosted Template URL: `http://host.docker.internal:8001/render`. Template params: `iframe_host`: `localhost`, `sync_service_url`: `http://localhost:8001`, `api_url`: `http://localhost:8080`.
  - Start this service with `--host 0.0.0.0`. Ensure port 8001 is reachable.

- **If both run in Docker**
  - Put both services on the same Docker network and set the Hosted Template URL to the service name and port, e.g. `http://fiftyone-sync:8001/message`.

## MongoDB (compose stack)

The compose stack in **`containers/fiftyone-sync`** launches a single MongoDB for the service. **Project 1** uses database `fiftyone_project_1` by default; other projects use `fiftyone_project_2`, etc., on the same instance.

```bash
# From repo root
docker compose -f containers/fiftyone-sync/compose.yaml up -d
```

Set `FIFTYONE_DATABASE_URI=mongodb://localhost:27017` when running the API on the host (or `mongodb://mongo:27017` from a container on the same network). See `containers/fiftyone-sync/.env.example`.

## Database and port allocation

Single MongoDB (launched via `containers/fiftyone-sync`); each Tator project gets its own database for isolation. One port per project: project 1 → 5151, project 2 → 5152, etc.

| Env var | Purpose |
|--------|---------|
| `FIFTYONE_DATABASE_URI` | MongoDB connection URI (default `mongodb://localhost:27017`). Override for remote/alternative MongoDB. |
| `FIFTYONE_DATABASE_DEFAULT` | Prefix for per-project database names. Default `fiftyone_project` → `fiftyone_project_1`, `fiftyone_project_2`, etc. |
| `FIFTYONE_DATABASE_NAME` | Override: use this single database name for all projects (ignores default pattern). |

Optional query param **`database_name`** on `GET /launch` and `POST /sync` overrides the database for that project.

## Sync and FiftyOne Dataset

`POST /sync` fetches Tator media and localizations, crops bounding boxes, builds a FiftyOne dataset, and launches the FiftyOne app.

### Query parameters

| Param | Required | Description |
|-------|----------|-------------|
| `project_id` | yes | Tator project ID |
| `api_url` | yes | Tator REST API base URL |
| `token` | yes | Tator API token |
| `version_id` | no | Version ID filter for localizations |
| `database_name` | no | Override MongoDB database name |
| `config_path` | no | Path to YAML/JSON config file for dataset build |
| `launch_app` | no | Launch FiftyOne app after sync (default: true) |

**Sync-from-Tator flow (dashboard):** The launcher template calls `POST /sync`, then opens the FiftyOne app in a new tab. The "Open Voxel51" link always points to the base URL from `FIFTYONE_APP_PUBLIC_BASE_URL` (e.g. `https://cortex.shore.mbari.org/fiftyone/`) — no port or dataset path is appended. The dataset name is always `project_name + "_v" + version_id + "_" + port` (e.g. `MyProject_v66_5151`). It cannot be overridden via config.

### Config file (YAML/JSON)

Use `config_path` to pass a config file path:

```yaml
media_id_batch_size: 200              # chunk size for get_media_list_by_id
localization_batch_size: 5000         # page size for localization list API

include_classes: [Larvacean, Copepod]   # optional: filter labels
image_extensions: ["*.png", "*.jpg"]
max_samples: 500                         # optional: limit for testing
```

The FiftyOne dataset name is always `project_name_v{version_id}_{port}` and cannot be set in config.

### Embeddings, UMAP, and similarity search

You can optionally compute **embeddings**, a **UMAP** 2D visualization, and a **similarity** index after the dataset is built. Embeddings are fetched from the **embed service** at `{service_url}/embed/{project}`. By default `{project}` is the **Tator project ID** (many services expect this). Set `embeddings.project_name` in config to use a project name or other key instead. Add an `embeddings` block to your config and pass it via `config_path`:

```yaml
embeddings:
  embeddings_field: embeddings         # field to store embedding vectors
  brain_key: umap_viz                 # FiftyOne brain key for UMAP
  umap_seed: 51
  force_embeddings: false             # set true to recompute embeddings
  force_umap: false                   # set true to recompute UMAP
  # Similarity search (fob.compute_similarity): omit or set to "" to disable
  similarity_brain_key: similarity_cosine
  similarity_metric: cosine            # e.g. cosine, euclidean
  force_similarity: false             # set true to recompute similarity index
  batch_size: 32                     # batch size for embed service requests
  service_url: null                   # optional; default FASTVSS_API_URL or http://localhost:8000
  project_name: null                  # optional; override for embed service URL path (default: project_id)
```

**Requirements:** The embed service must be running (e.g. Fast-VSS at the URL above). Set `FASTVSS_API_URL` to override the base URL. For UMAP visualization, install `umap-learn` in the sync service venv:

```bash
pip install umap-learn
```

If the embed service is unavailable or UMAP is not installed, sync still runs; embeddings/UMAP/similarity are skipped and a message is logged. Embeddings, UMAP, and similarity results are cached on the dataset; use `force_embeddings` / `force_umap` / `force_similarity` to recompute.

### Data layout

- Media: `/tmp/fiftyone_sync_project_{id}/download/{media_id}_{name}.jpg`
- Localizations: `/tmp/fiftyone_sync_project_{id}/localizations.jsonl` (JSONL)
- Crops: `/tmp/fiftyone_sync_project_{id}/crops/{media_stem}/{elemental_id}.png`

Labels come from `attributes.Label` (or `attributes.label`) in localizations.

### Sync edits back to Tator

`POST /sync-to-tator` pushes FiftyOne dataset edits (labels, confidence) back to Tator localizations. Run after editing in the FiftyOne app.

| Param | Required | Description |
|-------|----------|-------------|
| `project_id` | yes | Tator project ID |
| `version_id` | yes | Tator version ID (localizations must be in this version) |
| `api_url` | yes | Tator REST API base URL |
| `token` | yes | Tator API token |
| `dataset_name` | no | FiftyOne dataset name (default: `project_name_v{version_id}_{port}`) |

```bash
curl -X POST "http://localhost:8001/sync-to-tator?project_id=4&version_id=1&api_url=https://tator.example.com&token=YOUR_TOKEN"
```

Returns `{"status": "ok", "updated": N, "failed": M, "errors": [...]}`.

## Embedding API Usage

```bash
# Submit batch of images (project maps to Fast-VSS /embeddings/{project}/)
curl -X POST http://localhost:8000/embed \
  -F "files=@image1.jpg" \
  -F "files=@image2.jpg" \
  -F "project=testproject"
# Returns: {"uuid": "..."}

# Fetch results
curl http://localhost:8000/embed/{uuid}
# Returns: {"status": "completed", "embeddings": [[...], [...]]}
```
