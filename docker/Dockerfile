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
ENV PATH=$PATH:"/home/${USER_NAME}/.local/bin"
ENV DB_LOCATION="/myapp/db"

SHELL ["/bin/bash", "-c"]

# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates rsync python3 python3-pip && \
    # Clean package manager:
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    # Create a non-root user and give it rights over a "home" folder:
    adduser --disabled-password --gecos '' --uid ${USER_ID} ${USER_NAME} && \
    chown -R ${USER_NAME}: /home/${USER_NAME} && \
    mkdir -m 777 -p ${DB_LOCATION}

# Set user
USER ${USER_NAME}
# Change working directory
WORKDIR /home/${USER_NAME}

COPY --chown=${USER_NAME} ./ ./

RUN python3 -m pip install --upgrade pip

RUN python3 -m pip install -r requirements.txt
RUN python3 -m pip install .

CMD ["/bin/bash", "-c", "romi_scanner_rest_api"]