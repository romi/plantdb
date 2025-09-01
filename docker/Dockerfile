# ============= Builder stage for uwsgi =============
FROM ubuntu:22.04 AS builder

# Set up environment for building uwsgi
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
# Variables required by `pip`:
ENV PATH="/root/.local/bin:${PATH}"

# Install build dependencies
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
        # Install python3.10 & pip
        python3 python3-pip python3-dev \
        # Install 'gcc', 'libc6-dev' & 'python3-dev' to be able to build uwsgi
        gcc libc6-dev \
        # Install PCRE for uWSGI routing support
        libpcre3-dev \
        # Install build tools for compiling Python packages
        build-essential && \
    # Clean package manager:
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* 2>/dev/null || true

# Copy the source files starting from the root folder (relative to context build):
COPY src/commons commons/
COPY src/server server/

# Build uwsgi & Python packages
RUN python3 -m pip install --no-cache-dir --user \
        wheel setuptools pip --upgrade && \
    python3 -m pip install --no-cache-dir --user \
        uwsgi \
        commons/.[io] \
        server/. && \
    # Remove pip cache and unnecessary files
    python3 -m pip cache purge

# ============= Test stage =============
FROM builder AS test

# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
        # Install Open3D system dependencies
        libegl1 libgl1 libgomp1

# Install test dependencies
RUN echo "\n\n# Installing 'plantdb.commons' test dependencies..." && \
    python3 -m pip install --no-cache-dir --user \
        commons/[test] && \
    echo "\n\n# Installing 'plantdb.server' test dependencies..." && \
    python3 -m pip install --no-cache-dir --user \
        server/[test]
# Run tests
RUN echo "\n\n# Running 'plantdb.commons' tests..." && \
    nose2 -v -s commons/tests/ && \
    echo "\n\n# Running 'plantdb.commons' tests..." && \
    nose2 -v -s server/tests/

# ============= Final stage =============
FROM ubuntu:22.04 AS runtime
#FROM python:3.9-slim-bullseye
LABEL maintainer="Jonathan LEGRAND <jonathan.legrand@ens-lyon.fr>"
LABEL corresponding_author="Peter Hanappe <peter@romi-project.eu>"
LABEL project="Robotics for microfarms"
LABEL homepage="https://docs.romi-project.eu/documentation/"
LABEL repository="https://github.com/romi/plantdb"
LABEL license="LGPL-3.0-or-later"
LABEL description="A minimal REST API serving the PlantDB."

# Set user name and id variables:
ENV USER_NAME="romi"
ENV USER_ID=2020

# Variables required by Python:
ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1
# Variables required by `pip`:
ENV PATH="/home/${USER_NAME}/.local/bin:${PATH}"
# Fix pip problem when installed by Ubuntu22.04 (Jammy).
# See: https://github.com/pypa/setuptools/issues/3269#issuecomment-1254507377
ENV DEB_PYTHON_INSTALL_LAYOUT=deb_system

# URI prefix to use when behind a reverse proxy (NGINX)
ENV PLANTDB_API_PREFIX=""
# Location of the database folder to serve:
ENV ROMI_DB="/myapp/db"

# Change Shell to 'bash', default is 'sh':
SHELL ["/bin/bash", "-c"]

# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
        # Install python3.10 & pip
        python3 python3-distutils python3-dev \
        # Install Open3D system dependencies
        libegl1 libgl1 libgomp1 && \
    # Clean package manager:
    apt-get clean -y && \
    apt-get autoremove -y && \
    # Cleanup package manager and temp files
    rm -rf /var/lib/apt/lists/* 2>/dev/null || true && \
    rm -rf /tmp/* /var/tmp/* 2>/dev/null || true && \
    rm -rf /var/cache/apt/archives/*.deb 2>/dev/null || true && \
    # Remove man pages and docs
    find /usr/share/man /usr/share/doc /usr/share/info -delete 2>/dev/null || true && \
    # Keep only C.UTF-8 locale
    cd /usr/share/locale/ && \
    find . -mindepth 1 -maxdepth 1 ! -name 'C.UTF-8' -exec rm -rf {} + 2>/dev/null || true && \
    # Create a non-root user and create a "home" folder for it:
    adduser --disabled-password --gecos '' --uid ${USER_ID} ${USER_NAME} && \
    chown -R ${USER_NAME}: /home/${USER_NAME} && \
    # Create the folder used to mount the database:
    mkdir -p ${ROMI_DB} && \
    touch ${ROMI_DB}/romidb && \
    chown -R ${USER_NAME} ${ROMI_DB}

# Change to non-root user:
USER ${USER_NAME}
# Change working directory:
WORKDIR /home/${USER_NAME}

# Copy Python packages from builder (including uwsgi)
COPY --chown=${USER_NAME} --from=builder /root/.local /home/${USER_NAME}/.local

RUN echo "\n\n# Installing 'plantdb.commons', 'plantdb.server' & uwsgi..." && \
    python3 -m pip install --no-cache-dir --user plantdb.commons plantdb.server uwsgi && \
    # Clean pip cache:
    python3 -m pip cache purge && \
    # Clean unnecessary files:
    find /home/${USER_NAME}/.local -name "*.pyc" -delete 2>/dev/null || true && \
    find /home/${USER_NAME}/.local -name "__pycache__" -type d -exec rm -rf {} 2>/dev/null || true +

COPY --chown=${USER_NAME} docker/entrypoint.sh .
ENTRYPOINT ["./entrypoint.sh"]