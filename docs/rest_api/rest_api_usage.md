# How to Run the REST API

Below is a quick‑start guide that covers the most common ways to launch the **PlantDB REST API**.  
All the commands work on any platform where Python 3.10+ is available and the `plantdb.server` package has been installed (_e.g._ via `pip install plantdb.server` or from source).

## Core concepts

| Concept                    | What it is                                                                     | Where to configure it                                            |
|----------------------------|--------------------------------------------------------------------------------|------------------------------------------------------------------|
| **Database location**      | Path to the local FSDB that stores scans, images, point clouds, etc.           | `ROMI_DB` environment variable **or** `--db_location` CLI option |
| **API URL prefix**         | Optional prefix (_e.g._ `/api/v1`) that will be added in front of every endpoint | `PLANTDB_API_PREFIX` env‑var (used by the WSGI entry‑point)      |
| **SSL**                    | Enforce HTTPS‑only cookies (important when the server is exposed publicly)     | `PLANTDB_API_SSL=true`                                           |
| **Authentication secrets** | Randomly generated if omitted; control session and JWT handling                | `FLASK_SECRET_KEY` and `JWT_SECRET_KEY`                          |
| **Session limits**         | Maximum concurrent sessions, JWT lifetime, refresh token TTL                   | `MAX_SESSION`, `SESSION_TIMEOUT`, `REFRESH_TIMEOUT`              |
| **Proxy support**          | Required when the service sits behind a reverse proxy (_e.g._ Nginx, uWSGI)      | `--proxy` flag (CLI)                                             |

All of the above are documented in the module doc‑strings of **`fsdb_rest_api.py`** (the CLI) and **`wsgi.py`** (the WSGI entry point).

## Test mode – Play with a temporary toy dataset

If you just want to explore the API without touching any real data, start the server in **test mode**.
A temporary database (populated with a small sample dataset) is created automatically and removed when the process exits.

```shell
python fsdb_rest_api.py --test --debug
```

| Option                  | Effect                                                                 |
|-------------------------|------------------------------------------------------------------------|
| `--test`                | Builds a temporary FSDB (includes sample scans, images, point clouds). |
| `--debug`               | Runs Flask’s built‑in debugger (auto‑reload, detailed error pages).    |
| `--empty` *(optional)*  | Skip loading the sample dataset – you get an **empty** database.       |
| `--models` *(optional)* | Add pre‑trained CNN models to the temporary DB (useful for ML demos).  |

The server will listen on `0.0.0.0:5000` by default, which you can reach at **`http://localhost:5000/`**.

## Development – Run against a real local FSDB

When you have an existing PlantDB directory that you want to expose, start the server with explicit host/port settings:

```shell
python fsdb_rest_api.py \
    --db_location /path/to/your/database \
    --host 127.0.0.1 \
    --port 8080 \
    --debug          # optional: turn on Flask debug mode
```

**Key CLI flags**

| Flag            | Description                                                                                      |
|-----------------|--------------------------------------------------------------------------------------------------|
| `--db_location` | Path to the FSDB you wish to serve. If omitted, the environment variable `ROMI_DB` is consulted. |
| `--host`        | Network interface to bind (default `0.0.0.0`).                                                   |
| `--port`        | TCP port (default `5000`).                                                                       |
| `--proxy`       | Tell the app it is behind a reverse proxy; the server will respect the `X‑Forwarded-*` headers.  |
| `--log-level`   | Choose among `debug`, `info`, `warning`, `error`, `critical` (defaults to `INFO`).               |

**Example**: Behind a reverse proxy (e.g. Nginx)

```shell
python fsdb_rest_api.py \
    --db_location /data/plantdb \
    --host 127.0.0.1 \
    --port 5000 \
    --proxy
```

The proxy will forward requests to `127.0.0.1:5000`; the app will automatically strip the proxy prefix (if set via
`PLANTDB_API_PREFIX`) and generate correct URLs in responses.


## Production – Deploy with a WSGI server (uWSGI, Gunicorn)

For a robust, multi‑process deployment you typically use a dedicated WSGI server.
The **`wsgi.py`** module provides the ready‑to‑import `application` object:

```shell
uwsgi --http :5000 \
      --module plantdb.server.cli.wsgi:application \
      --callable application \
      --master
```

| Parameter                                      | Meaning                                                      |
|------------------------------------------------|--------------------------------------------------------------|
| `--http :5000`                                 | Expose the service on port 5000 (change as needed).          |
| `--module plantdb.server.cli.wsgi:application` | Import the WSGI entry point.                                 |
| `--callable application`                       | The Flask app instance created by `rest_api()`.              |
| `--master`                                     | Run uWSGI in master mode (recommended for graceful reloads). |

**Environment variables** (set before starting uWSGI) let you customise the deployment without touching code:

```shell
export ROMI_DB=/srv/plantdb
export PLANTDB_API_PREFIX=/api/v1
export PLANTDB_API_SSL=true          # enforce HTTPS‑only cookies
export FLASK_SECRET_KEY=myflasksecret
export JWT_SECRET_KEY=myjwtsecret
export SESSION_TIMEOUT=1800         # 30 min
export REFRESH_TIMEOUT=86400        # 1 day
export MAX_SESSION=20               # up to 20 concurrent users
```

The server will then be reachable at **`https://<host>:5000/api/v1/`** (or without the prefix if you leave
`PLANTDB_API_PREFIX` empty).

## Quick reference of the most useful endpoints

| Endpoint                                       | Method           | Description                                      |
|------------------------------------------------|------------------|--------------------------------------------------|
| `/`                                            | `GET`            | Health check & basic landing page.               |
| `/health`                                      | `GET`            | Detailed service health (database connectivity). |
| `/scans`                                       | `GET`            | List all scans (optionally filter by owner).     |
| `/scans_info`                                  | `GET`            | Tabular view with additional metadata.           |
| `/scan/<scan_id>`                              | `GET/PUT/DELETE` | CRUD operations on a single scan.                |
| `/scan/<scan_id>/filesets`                     | `GET/POST`       | List or create filesets belonging to a scan.     |
| `/file/<scan_id>/<fileset_id>/<file_id>`       | `GET/PUT/DELETE` | Access or modify a concrete file.                |
| `/image/<scan_id>/<fileset_id>/<file_id>`      | `GET`            | Retrieve an image asset.                         |
| `/pointcloud/<scan_id>/<fileset_id>/<file_id>` | `GET`            | Retrieve a point‑cloud asset.                    |
| `/mesh/<scan_id>/<fileset_id>/<file_id>`       | `GET`            | Retrieve a mesh asset.                           |
| `/register`                                    | `POST`           | Create a new user account.                       |
| `/login`                                       | `POST`           | Obtain a JWT access token.                       |
| `/logout`                                      | `POST`           | Invalidate the current JWT.                      |
| `/token-refresh`                               | `POST`           | Refresh an access token using a refresh token.   |
| `/create-api-token`                            | `POST`           | Generate a long‑lived API token (for scripts).   |

All routes respect the optional **URL prefix** (
`PLANTDB_API_PREFIX`) and enforce JWT authentication when the corresponding resource requires it.

## Common troubleshooting tips

| Symptom                                              | Likely cause                                                                                  | Fix                                                                                                                                             |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| **“No secret key was provided”** warnings on startup | Environment variables `FLASK_SECRET_KEY` / `JWT_SECRET_KEY` not set.                          | Either export those variables or let the server generate a random 64‑bit secret (the warning can be ignored for local testing).                 |
| **“Connecting to local plant database …” fails**     | Wrong `ROMI_DB` path or missing read permissions.                                             | Verify the path exists and is readable; set `ROMI_DB` correctly or use `--db_location`.                                                         |
| **All endpoints return 404**                         | The server is running behind a proxy but `--proxy` flag (or `PLANTDB_API_PREFIX`) is not set. | Restart with `--proxy` and/or configure `PLANTDB_API_PREFIX`.                                                                                   |
| **CORS errors from a web front‑end**                 | Cross‑origin requests blocked.                                                                | The Flask app enables CORS globally; ensure the browser is not caching old headers, or restrict origins via Flask‑CORS configuration if needed. |
| **SSL not enforced**                                 | `PLANTDB_API_SSL` is not true, or cookies are still sent over HTTP.                           | Set `PLANTDB_API_SSL=true` and serve the app via HTTPS (e.g., terminate TLS at the reverse proxy).                                              |
