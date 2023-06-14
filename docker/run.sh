#!/bin/bash
# - Defines colors and message types:
RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
NC="\033[0m" # No Color
INFO="${GREEN}INFO${NC}    "
WARNING="${YELLOW}WARNING${NC} "
ERROR="${RED}ERROR${NC}   "
bold() { echo -e "\e[1m$*\e[0m"; }

# - Default variables
# Image tag to use, 'latest' by default:
vtag="latest"
# Command to use to run unit tests:
unittest="nose2 -s tests/ --with-coverage"
# Command to execute after starting the docker container:
cmd=''
# Volume mounting options:
mount_option=""
# If the `DB_LOCATION` variable is set, use it as default database location, else set it to empty:
if [ -z ${DB_LOCATION+x} ]; then
  echo -e "${WARNING}Environment variable DB_LOCATION is not defined, set it to use as default database location!"
  host_db=''
else
  host_db=${DB_LOCATION}
fi

usage() {
  echo -e "$(bold USAGE):"
  echo "  ./docker/run.sh [OPTIONS]"
  echo ""

  echo -e "$(bold DESCRIPTION):"
  echo "  Run 'roboticsmicrofarms/plantdb' container with a mounted local (host) database and expose it to port 5000."
  echo ""

  echo -e "$(bold OPTIONS):"
  echo "  -t, --tag
    Docker image tag to use, default to '${vtag}'."
  echo "  -c, --cmd
    Defines the command to run at container startup.
    By default, starts the container and serve the database using the REST API on port 5000.
    Use '-c bash' to access the shell inside the docker container."
  echo "  -db, --database
    Path to the host database to mount inside docker container.
    Defaults to the value of the environment variable '${DB_LOCATION}', if any."
  echo "  -v, --volume
    Volume mapping for docker, e.g. '-v /abs/host/dir:/abs/container/dir'.
    Multiple use is allowed."
  echo " --unittest
    Runs unit tests defined in 'plantdb/tests/'."
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
    cmd=${unittest}
    mount_tests="${PWD}/tests:/myapp/tests" # mount the `plantdb/tests/` folder into the container
    mount_option="${mount_option} -v ${mount_tests}" # append
    ;;
  -v | --volume)
    shift
    mount_option="${mount_option} -v $1" # append
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

# Use the "host database path" (`${host_db}`) to create a bind mount to `/myapp/db`:
if [ "${host_db}" != "" ]; then
  mount_option="${mount_option} -v ${host_db}:/myapp/db"
fi

# If a 'host database path' is provided, get the name of the group and its id to, later used with the `--user` option
if [ "${host_db}" != "" ]; then
  group_name=$(stat -c "%G" ${host_db})                              # get the name of the group for the 'host database path'
  gid=$(getent group ${group_name} | cut --delimiter ':' --fields 3) # get the 'gid' of this group
#  echo "Automatic group name definition to '$group_name'!"
#  echo "Automatic group id definition to '$gid'!"
else
  group_name='myuser'
  gid=1000
fi

# Check if we have a TTY or not
if [ -t 1 ]; then
  USE_TTY="-it"
else
  USE_TTY=""
fi

if [ "${cmd}" = "" ]; then
  # Start in interactive mode:
  docker run --rm -p 5000:5000 ${mount_option} \
    --user myuser:${gid} \
    ${USE_TTY} roboticsmicrofarms/plantdb:${vtag} # try to keep the `-it` to be able to kill the process/container!
else
  # Start in non-interactive mode (run the command):
  docker run --rm -p 5000:5000 ${mount_option} \
    --user myuser:${gid} \
    ${USE_TTY} roboticsmicrofarms/plantdb:${vtag} \
    bash -c "${cmd}" # try to keep the `-it` to be able to kill the process/container!
fi
