#!/bin/bash

vtag="latest"
host_db="/data/ROMI/DB"
unittest="nose2 -s plantdb/tests/ --with-coverage"
cmd=""

usage() {
  echo "USAGE:"
  echo "  ./docker/run.sh [OPTIONS]
    "

  echo "DESCRIPTION:"
  echo "  Run 'roboticsmicrofarms/plantdb' container with a mounted local (host) database and expose it to port 5000.
  It must be run from the 'plantdb' repository root folder!
  "

  echo "OPTIONS:"
  echo "  -t, --tag
    Docker image tag to use, default to '$vtag'."
  echo "  -c, --cmd
    Defines the command to run at container startup.
    By default start an active container serving the database trough the REST API on port 5000.
    Use '-c /bin/bash' to access the shell inside the docker container."
  echo "  -db, --database
    Path to the host database to mount inside docker container, default to '$host_db'."
  echo " --unittest
    Runs unit tests defined in plantdb/tests/."
  echo "  -h, --help
    Output a usage message and exit."
}

while [ "$1" != "" ]; do
  case $1 in
  -t | --tag)
    shift
    vtag=$1
    ;;
  -c | --cmd)
    shift
    cmd=$1
    ;;
  -db | --database)
    shift
    host_db=$1
    ;;
  --unittest)
    cmd=$unittest
    ;;
  -h | --help)
    usage
    exit
    ;;
  *)
    usage
    exit 1
    ;;
  esac
  shift
done

# Check if we have a TTY or not
if [ -t 1 ]; then
  USE_TTY="-it"
else
  USE_TTY=""
fi

if [ "$cmd" = "" ]; then
  # Start in interactive mode:
  docker run \
    -p 5000:5000 \
    -v $host_db:/myapp/db \
    $USE_TTY roboticsmicrofarms/plantdb:$vtag # keep the `-it` to be able to kill the container!
else
  # Start in non-interactive mode (run the command):
  docker run \
    -p 5000:5000 \
    -v $host_db:/myapp/db \
    roboticsmicrofarms/plantdb:$vtag \
    bash -c "$cmd"
fi
