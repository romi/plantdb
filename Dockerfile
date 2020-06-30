FROM ubuntu:18.04

LABEL maintainer="Jonathan LEGRAND <jonathan.legrand@ens-lyon.fr>"

ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1

# Set non-root user name:
ENV SETUSER=scanner

USER root
# Change shell to 'bash', default is 'sh'
SHELL ["/bin/bash", "-c"]
# Update package manager & install requirements:
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
    git ca-certificates \
    python3.7 python3-pip python3-wheel && \
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m ${SETUSER} && \
    cd /home/${SETUSER} && \
    mkdir project && \
    chown -R ${SETUSER}: /home/${SETUSER}

# Change user
USER ${SETUSER}
# Change working directory:
WORKDIR /home/${SETUSER}/project

RUN git clone https://github.com/romi/romidata.git && \
    cd romidata && \
    git checkout dev && \
    python3.7 -m pip install setuptools setuptools-scm && \
    python3.7 -m pip install luigi pillow && \
    python3.7 -m pip install flask flask-restful flask-cors && \
    python3.7 -m pip install . && \
    mkdir ~/db

ENV DB_LOCATION="/home/${SETUSER}/db"
ENV PATH=$PATH:"/home/${SETUSER}/.local/bin"

CMD ["/bin/bash", "-c", "romi_scanner_rest_api"]