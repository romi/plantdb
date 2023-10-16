FROM ubuntu:20.04
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
ENV USER_ID=1000
# Required by `pip`:
ENV PATH=$PATH:"/home/${USER_NAME}/.local/bin"
# Required by `romi_run_task`:
ENV ROMI_DB="/myapp/db"

# Change Shell to 'bash', default is 'sh':
SHELL ["/bin/bash", "-c"]

# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates \
    rsync python3 python3-pip \
    libgomp1 && \
    # Clean package manager:
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    # Create a `romi` group with GID 2020:
    addgroup --gid 2020 romi && \
    # Create a non-root user, member of the romi group (2020), and give it rights over a "home" folder:
    adduser --disabled-password --gecos '' --ingroup romi --uid ${USER_ID} ${USER_NAME} && \
    chown -R ${USER_NAME}: /home/${USER_NAME} && \
    # Create the folder used to mount the database:
    mkdir -m 777 -p ${ROMI_DB}

# Change to non-root user:
USER ${USER_NAME}
# Change working directory:
WORKDIR /home/${USER_NAME}
# Copy the root foler (relative to context build):
COPY --chown=${USER_NAME} ./ ./plantdb/

RUN cd plantdb/ && \
    # Upgrade pip installed by system package manager (apt):
    python3 -m pip install --user --upgrade pip && \
    # Install `plantdb` dependencies and sources:
    python3 -m pip install --user -r requirements.txt && \
    python3 -m pip install --user . && \
    # Clean pip cache:
    rm -rf .cache/pip

CMD ["/bin/bash", "-c", "romi_scanner_rest_api"]