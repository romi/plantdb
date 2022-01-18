FROM ubuntu:18.04
LABEL maintainer="Jonathan LEGRAND <jonathan.legrand@ens-lyon.fr>"

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
    python3 -m pip install setuptools wheel && \
    python3 -m pip install -r plantdb/requirements.txt && \
    python3 -m pip install nose coverage && \
    python3 -m pip install -e plantdb/. && \
    rm -rf .cache/pip

CMD ["/bin/bash", "-c", "romi_scanner_rest_api"]