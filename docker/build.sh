#!/bin/bash
# - Defines colors and message types:
RED="\033[0;31m"
YELLOW="\033[0;33m"
GREEN="\033[0;32m"
NC="\033[0m" # No Color
INFO="${GREEN}INFO${NC}    "
WARNING="${YELLOW}WARNING${NC} "
ERROR="${RED}ERROR${NC}   "
bold() { echo -e "\e[1m$*\e[0m"; }

# - Default variables
# Image tag to use, 'latest' by default:
vtag="latest"
# String aggregating the docker build options to use:
docker_opts=""

usage() {
  echo -e "$(bold USAGE):"
  echo "  ./docker/build.sh [OPTIONS]"
  echo ""

  echo -e "$(bold DESCRIPTION):"
  echo "  Build a docker image named 'roboticsmicrofarms/plantdb' using 'Dockerfile' in the same location.
  It must be run from the 'plantdb' repository root folder as it is the build context and it will be copied during at image build time!"
  echo ""

  echo -e "$(bold OPTIONS):"
  echo "  -t, --tag
    Docker image tag to use, default to '${vtag}'."
  # -- Docker options:
  echo "  --no-cache
    Do not use cache when building the image, (re)start from scratch."
  echo "  --pull
    Always attempt to pull a newer version of the parent image."
  echo "  --plain
    Plain output during docker build."
  # -- General options:
  echo "  -h, --help
    Output a usage message and exit."
}

while [ "$1" != "" ]; do
  case $1 in
  -t | --tag)
    shift
    vtag=$1
    ;;
  --no-cache)
    docker_opts="${docker_opts} --no-cache"
    ;;
  --pull)
    docker_opts="${docker_opts} --pull"
    ;;
  --plain)
    docker_opts="${docker_opts} --progress=plain"
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

# Get the date to estimate docker image build time:
start_time=$(date +%s)
# Start the docker image build:
docker build -t roboticsmicrofarms/plantdb:${vtag} ${docker_opts} \
  -f docker/Dockerfile .
# Get docker build status:
docker_build_status=$?
# Get elapsed time:
elapsed_time=$(expr $(date +%s) - ${start_time})

# Print build time if successful (code 0), else print exit code
if [ ${docker_build_status} == 0 ]; then
  echo -e "\n${INFO}Docker build SUCCEEDED in ${elapsed_time}s!"
else
  echo -e "\n${ERROR}Docker build FAILED after ${elapsed_time}s with code ${docker_build_status}!"
fi
# Exit with docker build exit code:
exit ${docker_build_status}
