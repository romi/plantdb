FROM ubuntu:22.04
#FROM python:3.9-slim-bullseye
LABEL maintainer="Jonathan LEGRAND <jonathan.legrand@ens-lyon.fr>"
LABEL corresponding_author="Peter Hanappe <peter@romi-project.eu>"
LABEL project="Robotics for microfarms"
LABEL homepage="https://docs.romi-project.eu/documentation/"
LABEL repository="https://github.com/romi/plantdb"
LABEL license="LGPL-3.0-or-later"
LABEL description="A minimal REST API serving JSON information from the Plant DB."

ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1
ENV USER_NAME=romi
ENV USER_ID=1010
# Required by `pip`:
ENV PATH=$PATH:"/home/${USER_NAME}/.local/bin"
# https://github.com/pypa/setuptools/issues/3269#issuecomment-1254507377
ENV DEB_PYTHON_INSTALL_LAYOUT=deb_system
# Required by `romi_run_task`:
ENV ROMI_DB="/myapp/db"

# Change Shell to 'bash', default is 'sh':
SHELL ["/bin/bash", "-c"]

# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates rsync \
    python3 python3-pip \
    # Install Open3D system dependencies
    libegl1 libgl1 libgomp1 && \
    # Clean package manager:
    apt-get clean -y && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    # Create a non-root user named romi' and create a "home" folder wih proper rights:
    adduser --disabled-password --gecos '' --uid ${USER_ID} ${USER_NAME} && \
    chown -R ${USER_NAME}: /home/${USER_NAME} && \
    # Create the folder used to mount the database:
    mkdir -p ${ROMI_DB}

# Change to non-root user:
USER ${USER_NAME}
# Change working directory:
WORKDIR /home/${USER_NAME}
# Copy the source files starting from the root folder (relative to context build):
COPY --chown=${USER_NAME} ./ ./plantdb/

RUN cd plantdb/ && \
    # Install `plantdb` dependencies and sources:
    python3 -m pip install --user . && \
    # Clean pip cache:
    python3 -m pip cache purge

# By default, start the `fsdb_rest_api` CLI at container startup:
CMD ["/bin/bash", "-c", "fsdb_rest_api"]