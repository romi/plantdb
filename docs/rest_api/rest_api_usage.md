# How to Run the REST API

Below is a quick‑start guide that covers the most common ways to launch the **PlantDB REST API**.  
All the commands work on any platform where Python 3.10+ is available and the
`plantdb.server` package has been installed (_e.g._ via `pip install plantdb.server` or from source).

## Core concepts

| Concept                    | What it is                                                                       | Where to configure it                                  |
|----------------------------|----------------------------------------------------------------------------------|--------------------------------------------------------|
| **Database location**      | Path to the local FSDB that stores scans, images, point clouds, etc.             | `ROMI_DB` environment variable **or** `-db` CLI option |
| **API URL prefix**         | Optional prefix (_e.g._ `/api/v1`) that will be added in front of every endpoint | `API_PREFIX` env‑var (used by the WSGI entry‑point)    |
| **SSL**                    | Enforce HTTPS‑only cookies (important when the server is exposed publicly)       | `PLANTDB_API_SSL=true`                                 |
| **Authentication secrets** | Randomly generated if omitted; control session and JWT handling                  | `FLASK_SECRET_KEY` and `JWT_SECRET_KEY`                |
| **Session limits**         | Maximum concurrent sessions, JWT lifetime, refresh token TTL                     | `MAX_SESSION`, `SESSION_TIMEOUT`, `REFRESH_TIMEOUT`    |
| **Proxy support**          | Required when the service sits behind a reverse proxy (_e.g._ Nginx, uWSGI)      | `--proxy` flag (CLI)                                   |

All of the above are documented in the module docstrings of `fsdb_rest_api.py` (the CLI) and
`wsgi.py` (the WSGI entry point).

## Test mode - Play with a temporary toy dataset

If you just want to explore the API without touching any real data, start the server in **test mode**.
A temporary database (populated with a small sample dataset) is created automatically and removed when the process exits.

```shell
fsdb_rest_api --test --debug
```

| Option     | Effect                                                                 |
|------------|------------------------------------------------------------------------|
| `--test`   | Builds a temporary FSDB (includes sample scans, images, point clouds). |
| `--debug`  | Runs Flask’s built‑in debugger (auto‑reload, detailed error pages).    |
| `--empty`  | Skip loading the sample dataset, you get an **empty** database.        |
| `--models` | Add pre‑trained CNN models to the temporary DB (useful for ML demos).  |

The server will listen on `0.0.0.0:5000` by default, which you can reach at **`http://localhost:5000/`**.

## Development - Run against a real local FSDB

When you have an existing PlantDB directory that you want to expose, start the server with explicit host/port settings:

```shell
fsdb_rest_api \
    -db /path/to/your/database \
    --host 127.0.0.1 \
    --port 8080 \
    --debug          # optional: turn on Flask debug mode
```

### Key CLI flags

| Flag          | Description                                                                                      |
|---------------|--------------------------------------------------------------------------------------------------|
| `-db`         | Path to the FSDB you wish to serve. If omitted, the environment variable `ROMI_DB` is consulted. |
| `--host`      | Network interface to bind (default `0.0.0.0`).                                                   |
| `--port`      | TCP port (default `5000`).                                                                       |
| `--proxy`     | Tell the app it is behind a reverse proxy; the server will respect the `X‑Forwarded-*` headers.  |
| `--log-level` | Choose among `debug`, `info`, `warning`, `error`, `critical` (defaults to `INFO`).               |

### Example

Behind a reverse proxy (_e.g._ Nginx):

```shell
fsdb_rest_api \
    -db /data/plantdb \
    --host 127.0.0.1 \
    --port 5000 \
    --proxy
```

The proxy will forward requests to `127.0.0.1:5000`; the app will automatically strip the proxy prefix (if set via
`API_PREFIX`) and generate correct URLs in responses.

## Production - Deploy with a WSGI server (uWSGI, Gunicorn)

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

**Environment variables** (set before starting uWSGI) let you customize the deployment without touching code:

```shell
export ROMI_DB=/srv/plantdb            # path to the database on the server
export API_PREFIX=/api/v1      # default
export PLANTDB_API_SSL=true            # enforce HTTPS‑only cookies
export FLASK_SECRET_KEY=myflasksecret  # OPTIONAL, generated if missing
export JWT_SECRET_KEY=myjwtsecret      # OPTIONAL, generated if missing
export SESSION_TIMEOUT=1800            # 30 min
export REFRESH_TIMEOUT=86400           # 1 day
export MAX_SESSION=20                  # up to 20 concurrent users
```

The server will then be reachable at **`https://<host>:5000/api/v1/`**.

## Common troubleshooting tips

| Symptom                                              | Likely cause                                                                          | Fix                                                                                                                                             |
|------------------------------------------------------|---------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| **“No secret key was provided”** warnings on startup | Environment variables `FLASK_SECRET_KEY` / `JWT_SECRET_KEY` not set.                  | Either export those variables or let the server generate a random 64‑bit secret (the warning can be ignored for local testing).                 |
| **“Connecting to local plant database …” fails**     | Wrong `ROMI_DB` path or missing read permissions.                                     | Verify the path exists and is readable; set `ROMI_DB` correctly or use `-db`.                                                                   |
| **All endpoints return 404**                         | The server is running behind a proxy but `--proxy` flag (or `API_PREFIX`) is not set. | Restart with `--proxy` and/or configure `API_PREFIX`.                                                                                           |
| **CORS errors from a web front‑end**                 | Cross‑origin requests blocked.                                                        | The Flask app enables CORS globally; ensure the browser is not caching old headers, or restrict origins via Flask‑CORS configuration if needed. |
| **SSL not enforced**                                 | `PLANTDB_API_SSL` is not true, or cookies are still sent over HTTP.                   | Set `PLANTDB_API_SSL=true` and serve the app via HTTPS (_e.g._, terminate TLS at the reverse proxy).                                            |
