# FiftyOne Sync Service

Backend service to sync data between Tator and Voxel51 for quickly editing localizations
and downstream model iteration.

See https://docs.mbari.org/internal/ai/videos/voxel51demo.gif for demo of the community Voxel51 tool.

Supports both [Voxel51 Community](https://github.com/voxel51/fiftyone) and Voxel51 Enterprise sync.  Syncing is done by version through a simple applet.

### Applet for Tator dashboard to sync to Voxel51. When syncing is done, a clickable link is provided.
![template_applet.png](https://github.com/mbari-org/fiftyone-sync/raw/main/docs/imgs/template_applet.png)
### Example Voxel51 embedding/grid view. Samples can be refined by lassoing an embedding cluster, or by filtering onmetadata (e.g. depth, label, confidence):
![embedding_grid.png](https://github.com/mbari-org/fiftyone-sync/raw/main/docs/imgs/embedding_grid.png)
### Example Voxel51 similarity search view
![sim_search.png](https://github.com/mbari-org/fiftyone-sync/raw/main/docs/imgs/sim_search.png)
### FastAPI
![fastapi.png](https://github.com/mbari-org/fiftyone-sync/raw/main/docs/imgs/fastapi.png)

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
        DBMgr["Database Manager\ndatabase_manager.py\ndatabase_uri_config.py"]
        SyncQueue["Sync Queue\nsync_queue.py"]
        SyncLock["Sync Lock\nsync_lock.py"]
        SyncWorker["Sync Worker (RQ)\nsync_worker.py"]
        SyncLogic["Sync Logic\nsync.py + sync_filters.py"]
        Mounts["Media Mounts\nmedia_mounts.py"]
        EmbViz["Embeddings / UMAP\nembeddings_viz.py"]
        CleanVision["CleanVision Filter\ncleanvision_filter.py"]
    end

    subgraph ExternalServices["External Services"]
        Tator["Tator REST API"]
        FastVSS["Fast-VSS\n(Embedding Service)"]
        Redis["Redis\n(Job Queue + Locks)"]
        MongoDB["MongoDB\n(FiftyOne DB per project)"]
        FOApp["FiftyOne App\n(FIFTYONE_APP_PUBLIC_BASE_URL)"]
        MountFS["Mounted Media Storage\n(read in place)"]
        S3["AWS S3\n(optional crop push)"]
    end

    %% Tator fetches the launcher template
    Gunicorn -->|"GET /render"| API
    API --> Launcher

    %% User interactions
    UI -->|"POST /sync\nPOST /sync-to-tator\nPOST /dimreduce\nPOST /recompute-crops"| API
    UI -->|"GET /versions /sections\n/localization-types /datasets\n/vss-projects /vss-embedding"| API
    UI -->|"GET .../status/{job_id}\nGET .../logs/{job_id}"| API
    UI -->|"Open FiftyOne"| FOTab
    FOTab --> FOApp
    FOApp -->|"read datasets"| MongoDB

    %% Job flow
    API -->|"enqueue job"| SyncQueue
    SyncQueue -->|"job"| Redis
    API -->|"poll job status / logs"| Redis
    Redis -->|"dequeue job"| SyncWorker
    SyncWorker --> SyncLogic
    SyncLogic --> SyncLock
    SyncLock -->|"one sync per version"| Redis

    %% Sync: Tator -> FiftyOne
    SyncLogic -->|"page media (offset)\n& localizations"| Tator
    SyncLogic --> Mounts
    Mounts -->|"resolve media URL\nto local path"| MountFS
    SyncLogic -->|"download media\n(unmounted / video)"| Tator
    SyncLogic -->|"write dataset"| MongoDB
    SyncLogic -->|"push crops (optional)"| S3
    SyncLogic --> CleanVision
    SyncLogic --> EmbViz
    EmbViz -->|"embed crops"| FastVSS

    %% Sync-to-Tator: FiftyOne edits -> Tator
    SyncLogic -->|"sync-to-tator:\nupdate localizations"| Tator

    %% Embeddings API
    API -->|"POST /embed\nGET /embed/{job_id}"| EmbedSvc
    EmbedSvc -->|"POST /embeddings/{project}/\nWS /ws/predict/job/{id}/{project}"| FastVSS

    %% Database / config
    API --> DBMgr
    DBMgr -->|"URI / database lookup"| MongoDB
```

## Features

- **Embedding API**: Delegates to Fast-VSS (`http://localhost:8000/embeddings/{project}/`)
  - `POST /embed` - Submit images (multipart/form-data) + project, returns UUID
  - `GET /embed/{uuid}` - Poll for results (job status from Fast-VSS via WebSocket `/ws/predict/job/{job_id}/{project}`)
  - Set `FASTVSS_API_URL` env var to override Fast-VSS base URL

- **FiftyOne App**: Served separately at `FIFTYONE_APP_PUBLIC_BASE_URL` and reads the same MongoDB; this service builds datasets but does not start App instances.

- **Per-project port key**: Every launcher request carries a `port` (first configured port for the project, else 5151 + (project_id - 1)). Together with the project name it selects the project's database entry in `config.yml` and is appended to default dataset names.

- **MongoDB isolation**: One MongoDB (`containers/fiftyone-sync`); each project uses the database from its `config.yml` `databases` entry, else `{FIFTYONE_DATABASE_DEFAULT}_{project_id}` (default `fiftyone_project_{id}`).

- **Launcher** (HostedTemplate): `/render` (Open FiftyOne + Sync from Tator), `/versions`, `/sections`, `/localization-types`, `/vss-projects`, `/database-info`, plus queued jobs `/sync`, `/sync-to-tator`, `/dimreduce` and `/recompute-crops`, each with `/status/{job_id}` and `/logs/{job_id}`. Token entered in applet via **Verify Token**; FiftyOne opens in a new tab (`iframe_host` = app host).

- **Dataset management**: `/datasets` (list), `/dataset-exists`, `/delete-dataset`, `/rename-dataset`. Datasets are named `project_v{version}[_s{section}]_{port}` by default for traceability; override at create time with `POST /sync?dataset_name=...` (applet **Dataset name** field). `POST /rename-dataset?new_name=...` optionally takes `dataset_name` as the current dataset to rename (otherwise the default version/section dataset is renamed). Names are sanitized to safe characters, max **60** characters. These four endpoints accept the Tator API token either via `Authorization: Token <token>` (or `Bearer <token>`) header, or as a `token` query parameter (the convention used by `/sync`, `/sync-to-tator`, `/recompute-crops`, `/dimreduce`).

- **Near-duplicate removal (CleanVision)**: `remove_near_duplicates=true` on `POST /sync` (or the **Remove near duplicates** checkbox in the applet) prunes near-duplicate, dark, and low-information crops from the dataset before embeddings are computed, keeping one image per near-duplicate set. Blur is deliberately not detected — the check misread soft-edged specimens as blurred photographs. Fewer samples means less annotator overhead and less memory/GPU pressure. **Voxel51 samples only** — nothing is deleted in Tator, and the removed crop images are *moved* into a `crops_removed/` folder beside the crops rather than deleted, so they can be reviewed or restored. Tunable via the `cleanvision` block in `config.yml`; see [docs/USAGE.md](docs/USAGE.md#near-duplicate-and-low-quality-sample-removal-cleanvision).

- **Resumable, batched media loading**: Re-running an interrupted sync skips media whose crops are already on disk and adds only the samples missing from a partially loaded dataset. Media download and dataset inserts run in batches (`FIFTYONE_SYNC_MEDIA_DOWNLOAD_BATCH`, `FIFTYONE_SYNC_DATASET_ADD_BATCH`) so syncs with millions of media stay memory-bounded. See [docs/USAGE.md](docs/USAGE.md#data-layout).

- **Mounted image storage**: A global `mounts` list in `config.yml` maps image URLs (`host` + `nginx_root`) onto a local path (e.g. `/mnt/DeepSea-AI`). Matching images are cropped in place — no download and no copy into the download folder — with automatic fallback to the Tator download when the file is not on the mount. See [docs/USAGE.md](docs/USAGE.md#mounted-image-storage).
- **Sync size cap**: Set `FIFTYONE_SYNC_MAX_IMAGES` (or config `max_samples`) to randomly subsample oversized Tator exports before cropping and dataset build, avoiding OOM on huge versions. See [docs/USAGE.md](docs/USAGE.md#sync-and-fiftyone-dataset).

- **Sync queue (Redis)**: Background worker: `python -m src.app.sync_worker`. Env: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_USE_SSL`, or `REDIS_URL`.

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

## Documentation

Full setup and API reference (config file, query parameters, embeddings/UMAP/similarity, AWS S3 crop upload, Testing, Hosted Template applet registration, database/port allocation, on-disk data layout, pushing edits back to Tator, standalone embedding API, and utility scripts) has moved to **[docs/USAGE.md](docs/USAGE.md)**.

Quick links:
- [AWS S3 crop upload (Enterprise only)](docs/USAGE.md#aws-s3-crop-image-upload-enterprise-only)
- [Testing](docs/USAGE.md#testing)
- [Hosted Template applet setup](docs/USAGE.md#hosted-template-applet-recommended)
- [Database and port allocation](docs/USAGE.md#database-and-port-allocation)
- [`/sync` API reference (query params, config file, embeddings/UMAP)](docs/USAGE.md#sync-and-fiftyone-dataset)
- [`/sync-to-tator` (push edits back to Tator)](docs/USAGE.md#sync-edits-back-to-tator)
- [Embedding API usage](docs/USAGE.md#embedding-api-usage)
- [Scripts](docs/USAGE.md#scripts)
