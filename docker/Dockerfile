FROM ubuntu:18.04
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

USER root
# Change shell to 'bash', default is 'sh'
SHELL ["/bin/bash", "-c"]
# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates rsync python3.7 python3-pip && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.7 10 && \
    # Clean package manager:
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    # Create a non-root user and give it rights over a "myapp" folder:
    adduser --disabled-password --gecos '' ${USER_NAME} && \
    mkdir -p ${DB_LOCATION} && chown -R ${USER_NAME} /myapp

# Change to non-root user:
USER ${USER_NAME}
# Change working directory:
WORKDIR /myapp
# Copy the sources:
COPY --chown=${USER_NAME} ./ plantdb/

# Install the required dependencies and plantdb:
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install poetry && \
    cd plantdb && \
    poetry env use 3.7 && \
    poetry install -n --no-dev && \
    poetry cache clear -n --all . && \
    rm -rf .cache/pip

CMD ["./plantdb/docker/docker-entrypoint.sh"]