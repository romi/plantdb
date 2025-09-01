#!/bin/bash

# --------------------------------
# Functions for colors and messages
# --------------------------------
setup_colors() {
  RED="\033[0;31m"    # Define red color code
  GREEN="\033[0;32m"  # Define green color code
  YELLOW="\033[0;33m" # Define yellow color code
  BLUE="\033[0;34m"   # Define blue color code for debug messages
  NC="\033[0m"        # No Color code to reset colors
  INFO="${GREEN}INFO${NC}    "    # Prefix for info messages
  WARNING="${YELLOW}WARNING${NC} " # Prefix for warning messages
  ERROR="${RED}$(bold ERROR)${NC}   " # Prefix for error messages using bold function
  DEBUG="${BLUE}DEBUG${NC}   "   # Prefix for debug messages
}

bold() {
  echo -e "\e[1m$*\e[0m" # Make text bold and reset
}

log_info() {
  echo -e "${INFO}$1" # Print info message with INFO prefix
}

log_warning() {
  echo -e "${WARNING}$1" # Print warning message with WARNING prefix
}

log_error() {
  echo -e "${ERROR}$1" # Print error message with ERROR prefix
}

log_debug() {
  if [ "${DEBUG_MODE}" = true ]; then
    echo -e "${DEBUG}$1" # Print debug message with DEBUG prefix if debug mode is enabled
  fi
}

# --------------------------------
# Functions for script initialization
# --------------------------------
initialize_variables() {
  # Image tag to use, 'latest' by default:
  VTAG="latest"
  # String aggregating the docker build options to use:
  DOCKER_OPTS=""
  # Running the Docker test stage is disabled by default
  RUN_TESTS=false
  TEST_ONLY=false
  # Debug mode is disabled by default
  DEBUG_MODE=false
}

# --------------------------------
# Usage information function
# --------------------------------
show_usage() {
  echo -e "$(bold USAGE):"
  echo "  ./docker/build.sh [OPTIONS]"
  echo ""

  echo -e "$(bold DESCRIPTION):"
  echo "  Build a docker image named 'roboticsmicrofarms/plantdb' using 'Dockerfile' in the same location.
  It must be run from the 'plantdb' repository root folder as it is the build context and it will be copied during at image build time!"
  echo ""

  echo -e "$(bold OPTIONS):"
  echo "  -t, --tag
    Docker image tag to use, default to '${VTAG}'."
  echo "  --test
    Run test stage prior to building the final image."
  echo "  --test-only
    Run test stage only (do not build final image)."
  # -- Docker options:
  echo "  --no-cache
    Do not use cache when building the image, (re)start from scratch."
  echo "  --pull
    Always attempt to pull a newer version of the parent image."
  echo "  --plain
    Plain output during docker build."
  # -- Debug option:
  echo "  --debug
    Enable debug mode to print additional debug information."
  # -- General options:
  echo "  -h, --help
    Output a usage message and exit."
}


# --------------------------------
# Command line parsing function
# --------------------------------
parse_arguments() {
  while [ "$1" != "" ]; do
    case $1 in
    -t | --tag)
      shift
      VTAG=$1
      ;;
    --test)
      RUN_TESTS=true
      log_debug "Test stage enabled!"
      ;;
    --test-only)
      RUN_TESTS=true
      TEST_ONLY=true
      log_debug "Test only mode enabled!"
    ;;
    --no-cache)
      DOCKER_OPTS="${DOCKER_OPTS} --no-cache"
      ;;
    --pull)
      DOCKER_OPTS="${DOCKER_OPTS} --pull"
      ;;
    --plain)
      DOCKER_OPTS="${DOCKER_OPTS} --progress=plain"
      ;;
    --debug)
      DEBUG_MODE=true
      log_debug "Debug mode enabled!"
      ;;
    -h | --help)
      show_usage
      exit
      ;;
    *)
      show_usage
      exit 1
      ;;
    esac
    shift
  done
}

# --------------------------------
# Docker build function
# --------------------------------
docker_build_cmd() {
  if [ "${RUN_TESTS}" = true ]; then
    local VTAG='test'
  fi
  # Construct the docker build command
  docker_cmd="docker build"
  docker_cmd+=" -t \"roboticsmicrofarms/plantdb:${VTAG}\""
  docker_cmd+=" ${DOCKER_OPTS}"  # Additional options like --no-cache, --pull, etc.
  if [ "${RUN_TESTS}" = true ]; then
      docker_cmd+=" --target test"
  fi
  docker_cmd+=" -f \"docker/Dockerfile\""
  docker_cmd+=" ."  # Build context
}

build_test_docker() {
  # Create the docker build command
  docker_build_cmd
  # Print the full command that will be executed
  log_debug "Executing command: ${docker_cmd}"

  log_info "Starting Docker build TEST stage..."
  # Get the date to estimate docker image build time:
  start_time=$(date +%s)
  # Execute the docker build command with test stage as target
  eval ${docker_cmd}

  # Get docker build status:
  docker_build_status=$?
  # Get elapsed time:
  elapsed_time=$(($(date +%s) - start_time))
  # Print build time if successful (code 0), else print exit code
  if [ ${docker_build_status} == 0 ]; then
    log_info "Docker TEST build SUCCEEDED in ${elapsed_time}s!"
    # Remove the test image after successful TEST build
    docker rmi "roboticsmicrofarms/plantdb:test" || log_warning "Could not remove test image"
    RUN_TESTS=false  # switch the variable to false after running the tests
  else
    log_error "Docker TEST build FAILED after ${elapsed_time}s with code ${docker_build_status}!"
    # Exit with docker build exit code:
    exit ${docker_build_status}
  fi

   # Check if it's test-only mode and exit if true
  if [ "${TEST_ONLY}" = true ]; then
    # Exit with docker build exit code:
    exit ${docker_build_status}
  fi
}

build_docker(){
  if [ "${RUN_TESTS}" = true ]; then
    build_test_docker
  fi

  # Create the docker build command
  docker_build_cmd

  # Print the build configuration options
  log_debug "Build configuration:"
  log_debug "- Docker tag: ${VTAG}"
  log_debug "- Docker options: ${DOCKER_OPTS}"
  # Print the full command that will be executed
  log_debug "Executing command: ${docker_cmd}"

  log_info "Starting Docker build..."
  # Get the date to estimate docker image build time:
  start_time=$(date +%s)
  # Execute the docker build command
  eval ${docker_cmd}

  # Get docker build status:
  docker_build_status=$?
  # Get elapsed time:
  elapsed_time=$(($(date +%s) - start_time))
  # Print build time if successful (code 0), else print exit code
  if [ ${docker_build_status} == 0 ]; then
    log_info "Docker build SUCCEEDED in ${elapsed_time}s!"
  else
    log_error "Docker build FAILED after ${elapsed_time}s with code ${docker_build_status}!"
  fi

  # Exit with docker build exit code:
  exit ${docker_build_status}
}

# --------------------------------
# Main script execution
# --------------------------------
main() {
  setup_colors
  initialize_variables
  parse_arguments "$@"
  build_docker
}

# Execute main function with all arguments
main "$@"