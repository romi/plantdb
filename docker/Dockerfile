FROM ubuntu:20.04 AS base
LABEL maintainer="Jonathan LEGRAND <jonathan.legrand@ens-lyon.fr>"
LABEL corresponding_author="Peter Hanappe <peter@romi-project.eu>"
LABEL project="Robotics for microfarms"
LABEL homepage="https://docs.romi-project.eu/documentation/"
LABEL repository="https://github.com/romi/plantdb"
LABEL license="LGPL-3.0-or-later"
LABEL description="A minimal REST API serving JSON information from the Plant DB."

ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1
ENV USER_NAME=myuser
ENV PATH=$PATH:"/home/${USER_NAME}/.local/bin"
ENV DB_LOCATION="/myapp/db"

WORKDIR /myapp
SHELL ["/bin/bash", "-c"]

FROM base AS builder

# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates python3 python3-pip python3-venv && \
    # Create a non-root user and give it rights over a "myapp" folder:
    adduser --disabled-password --gecos '' ${USER_NAME} && \
    chown -R ${USER_NAME} /myapp && \
    mkdir -p /venv && chown -R ${USER_NAME} /venv

USER ${USER_NAME}

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install poetry && \
    python3 -m venv /venv

COPY --chown=${USER_NAME} ./ ./
RUN source /venv/bin/activate && \
    poetry install -n --no-dev --no-root && \
    poetry build && \
    /venv/bin/pip install dist/*.whl

FROM base AS final

RUN adduser --disabled-password --gecos '' ${USER_NAME} && \
    mkdir -p ${DB_LOCATION} && \
    chown -R ${USER_NAME} /myapp && \
    apt-get update && \
    apt-get install -yq --no-install-recommends python3 rsync && \
    # Clean package manager:
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/*

USER ${USER_NAME}

COPY --from=builder --chown=${USER_NAME} /venv /venv
COPY ./docker/docker-entrypoint.sh ./

CMD ["./docker-entrypoint.sh"]