#!/bin/bash
# - Defines colors and message types:
RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
NC="\033[0m" # No Color
bold() { echo -e "\e[1m$*\e[0m"; }
INFO="${GREEN}INFO${NC}    "
WARNING="${YELLOW}WARNING${NC} "
ERROR="${RED}$(bold ERROR)${NC}   "

# - Default variables
# Image tag to use, 'latest' by default:
vtag="latest"
# Command to use to run unit tests:
unittest="nose2 -s plantdb/tests/"
# Command to execute after starting the docker container:
cmd=''
# Volume mounting options:
mount_option=""
# Port to use to serve the REST API
port='5000'
# If the `ROMI_DB` variable is set, use it as default database location, else set it to empty:
if [ -z ${ROMI_DB+x} ]; then
  host_db=''
else
  host_db=${ROMI_DB}
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
    Defaults to the value of the environment variable 'ROMI_DB', if any."
  echo "  -v, --volume
    Volume mapping for docker, e.g. '-v /abs/host/dir:/abs/container/dir'.
    Multiple use is allowed."
  echo " --unittest
    Runs unit tests defined in 'plantdb/tests/'."
  echo "  -h, --help
    Output a usage message and exit."
}

self_test=0  # 0/1 to indicate call to unittest!
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
    self_test=1
    #mount_tests="${PWD}/tests:/myapp/tests" # mount the `plantdb/tests/` folder into the container
    #mount_option="${mount_option} -v ${mount_tests}" # append
    ;;
  -v | --volume)
    shift
    mount_option="${mount_option} -v $1" # append
    ;;
  -p | --port)
    shift
    port=$1
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

# If the `ROMI_DB` variable is set, use it as default database location, else set it to empty:
if [ -z ${ROMI_DB+x} ]; then
  echo -e "${WARNING}Environment variable 'ROMI_DB' is not defined, set it to use as default database location!"
fi

# Use local database path `$host_db` to create a bind mount to '/myapp/db':
if [ "${host_db}" != "" ]; then
  mount_option="${mount_option} -v ${host_db}:/myapp/db"
  echo -e "${INFO}Automatic bind mount of '${host_db}' (host) to '/myapp/db' (container)!"
else
  # Only raise next ERROR message if not a SELF-TEST:
  if [ ${self_test} == 0 ]; then
    echo -e "${ERROR}No local host database defined!"
    echo -e "${INFO}Set 'ROMI_DB' or use the '-db' | '--database' option to define it."
    exit 1
  fi
fi

# If a 'host database path' is provided, get the name of the group and its id to, later used with the `--user` option
if [ "${host_db}" != "" ]; then
  group_name=$(stat -c "%G" ${host_db})                              # get the name of the group for the 'host database path'
  gid=$(getent group ${group_name} | cut --delimiter ':' --fields 3) # get the 'gid' of this group
  echo -e "${INFO}Using host database path group name '${group_name}' & '${gid}'."
else
  group_name='myuser'
  gid=1000
  # Only raise next WARNING message if not a SELF-TEST:
  if [ ${self_test} == 0 ]; then
    echo -e "${WARNING}Using default group name '${group_name}' & '${gid}'."
  fi
fi

# Check if we have a TTY or not
if [ -t 1 ]; then
  USE_TTY="-it"
else
  USE_TTY=""
fi

if [ "${cmd}" = "" ]; then
  # Start in interactive mode:
  docker run --rm -p ${port}:5000 ${mount_option} \
    --user myuser:${gid} \
    ${USE_TTY} roboticsmicrofarms/plantdb:${vtag} # try to keep the `-it` to be able to kill the process/container!
else
  # Get the date to estimate command execution time:
  start_time=$(date +%s)
  # Start in non-interactive mode (run the command):
  docker run --rm -p ${port}:5000 ${mount_option} \
    --user myuser:${gid} \
    ${USE_TTY} roboticsmicrofarms/plantdb:${vtag} \
    bash -c "${cmd}" # try to keep the `-it` to be able to kill the process/container!
  # Get command exit code:
  cmd_status=$?
  # Print build time if successful (code 0), else print command exit code
  if [ ${cmd_status} == 0 ]; then
    echo -e "\n${INFO}Command SUCCEEDED in $(expr $(date +%s) - ${start_time})s!"
  else
    echo -e "\n${ERROR}Command FAILED after $(expr $(date +%s) - ${start_time})s with code ${cmd_status}!"
  fi
  # Exit with status code:
  exit ${cmd_status}
fi
