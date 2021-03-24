FROM ubuntu:18.04

LABEL maintainer="Jonathan LEGRAND <jonathan.legrand@ens-lyon.fr>"

ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1
ARG USER_NAME=scanner
ARG PLANTDB_BRANCH=dev

USER root
# Change shell to 'bash', default is 'sh'
SHELL ["/bin/bash", "-c"]
# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates \
    python3.7 python3-pip python3-wheel && \
    # Clean package manager:
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    # Add user USER_NAME:
    useradd -m ${USER_NAME} && \
    cd /home/${USER_NAME} && \
    # Create 'ROMI' install directory
    mkdir ROMI && \
    chown -R ${USER_NAME}: /home/${USER_NAME}

# Change user
USER ${USER_NAME}
# Change working directory:
WORKDIR /home/${USER_NAME}/ROMI

RUN git clone --branch $PLANTDB_BRANCH https://github.com/romi/plantdb.git && \
    cd plantdb && \
    python3.7 -m pip install setuptools setuptools-scm --no-cache-dir && \
    python3.7 -m pip install . --no-cache-dir && \
    mkdir ~/db

ENV DB_LOCATION="/home/${USER_NAME}/db"
ENV PATH=$PATH:"/home/${USER_NAME}/.local/bin"

CMD ["/bin/bash", "-c", "romi_scanner_rest_api"]