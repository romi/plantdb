# Release Guide

This document describes the steps required to create a new release.

## Versioning Convention

Version numbers are expressed as `X.Y.Z`.

- **New features or API changes**: Increment the **major** (X) and/or **minor** (Y) number, and reset the 
  **patch level** (Z) to `0`.
- **Bug‑fix or hot‑fix releases**: Increment only the **patch level** (Z).

**WARNING**: do NOT add a v prior to the version number, like `vX.Y.Z`.


## Git‑Flow Workflow

The Git‑Flow model defines several branch types:

- **main** - Production code
- **dev** - Main integration branch for development
- **feature/*** - Branches for developing new features (branched from `dev`)
- **release/*** - Temporary branches to prepare releases (`dev` → `main`)
- **hotfix/*** - Branches to quickly fix critical production bugs (branched from `main`)

Git‑Flow also provides commands for managing these branch types:

```shell
git flow feature start feature-name          # Same as: git checkout -b feature/feature-name dev
git flow feature finish feature-name         # Merge into dev and delete the feature branch

git flow hotfix start X.Y.Z+1                # Create a hot‑fix branch from main
git flow hotfix finish X.Y.Z+1               # Merge into main AND dev, tag, then delete
```

---

# Release Steps - Git Flow

## 1. Preparation

Make sure your local `main` and `dev` branches are up‑to‑date with the remote repository:

```shell
git checkout main && git pull
git checkout dev && git pull
```

## 2. Create the Release

Start the release with:

```shell
git flow release start X.Y.Z
```

## 3. Update Files

1. Update the `CHANGELOG.md` to include the changes for this release:
   ```markdown
   # Version X.Y.Z - YYYY-MM-DD
      - Description of the first change
      - Description of the second change
      - ...
   ```

2. Update version numbers in TOML files:
    - `src/commons/pyproject.toml`
    - `src/client/pyproject.toml`
    - `src/server/pyproject.toml`


3. Commit the changes:
   ```shell
   git add CHANGELOG.md src/commons/pyproject.toml src/client/pyproject.toml src/server/pyproject.toml
   git commit -m "release X.Y.Z"
   ```

## 4. Finish the Release

Finish the release with:

```shell
git flow release finish X.Y.Z
```

> What this command does:
> - Merges the `release/X.Y.Z` branch into `main`
> - Adds a tag named after the version
> - Merges the release into `dev`
> - Deletes the `release/X.Y.Z` branch

## 5. Publish the Release

Push the updated branches and tag to the remote repository:

```shell
git push --follow-tags origin dev main
```

> Note: This command pushes both `dev` and `main` branches, as well as the version tag.

# Release Steps - Standard Git

## 1. Preparation

Make sure your local `main` and `dev` branches are up‑to‑date with the remote repository:

```shell
git checkout main && git pull
git checkout dev && git pull
```

## 2. Create the Release

Start the release with:

```shell
git checkout dev
git pull
git checkout -b release/X.Y.Z dev
```

## 3. Update Files

1. Update the `CHANGELOG.md` to include the changes for this release:
   ```markdown
   # Version X.Y.Z - YYYY-MM-DD
      - Description of the first change
      - Description of the second change
      - ...
   ```

2. Update version numbers in TOML files:
    - `src/commons/pyproject.toml`
    - `src/client/pyproject.toml`
    - `src/server/pyproject.toml`


3. Commit the changes:
   ```shell
   git add CHANGELOG.md src/commons/pyproject.toml src/client/pyproject.toml src/server/pyproject.toml
   git commit -m "release X.Y.Z"
   ```

## 4. Finish the Release

1. Ensure branches are up‑to‑date
   ```shell
   git checkout dev
   git pull
   git checkout main
   git pull
   ```

2. Merge release into main
   ```shell
   git checkout main
   git merge --no-ff release/X.Y.Z
   git tag -a X.Y.Z -m "Version X.Y.Z"
   ```

3. Merge release into dev
   ```shell
   git checkout dev
   git merge --no-ff release/X.Y.Z
   ```

4. Delete the release branch
   ```shell
   git branch -d release/X.Y.Z
   ```

## 5. Publish the Release

Push the updated branches and tag to the remote repository:

```shell
git push --follow-tags origin dev main
```

> Note: This command pushes both `dev` and `main` branches, as well as the version tag.

# Post‑Release Verification

Verify that:

- The tag was created and pushed correctly.
- `dev` and `main` contain the expected changes.
- CI/CD pipelines ran successfully.

> Note: Always replace `X.Y.Z` with the appropriate version number for your release.

---

# CI/CD Workflows

The following GitHub Actions are automatically triggered during a release (or manually via the **Actions
** tab) and help maintain code quality, build artifacts, and publish them to the respective platforms.

| Workflow                  | Trigger                                      | Short description                                                                                                                           |
|---------------------------|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| **pull_request.yml**      | `pull_request`                               | Runs unit tests on every PR, building a Docker image for the `test` stage and cleaning up afterwards.                                       |
| **gh‑pages.yml**          | `release` (published) or `workflow_dispatch` | Builds the MkDocs documentation and deploys it to GitHub Pages, ensuring docs stay in sync with the release.                                |
| **conda.yml**             | `release` (published) or `workflow_dispatch` | Builds and uploads Conda packages for `commons`, `server`, and `client` to Anaconda.org, using a matrix strategy to preserve build order.   |
| **pip_build_client.yml**  | `release` (published) or `workflow_dispatch` | Builds the `plantdb.client` package, stores the wheel/source distributions as artifacts, and publishes them to PyPI via trusted publishing. |
| **pip_build_server.yml**  | `release` (published) or `workflow_dispatch` | Same as `pip_build_client.yml` but for the `plantdb.server` package.                                                                        |
| **pip_build_commons.yml** | `release` (published) or `workflow_dispatch` | Same as `pip_build_client.yml` but for the `plantdb.commons` package.                                                                       |
| **docker.yml**            | `release` (published) or `workflow_dispatch` | Builds a multi‑platform Docker image, runs a simple validation test, and pushes the image (with SHA‑tag and `latest`) to Docker Hub.        |

These workflows run automatically when a new release is published, or they can be triggered manually from the 
**Actions** tab for ad‑hoc testing or re‑runs.
The documentation above gives you a quick reference to understand what each job does during the release process.  
