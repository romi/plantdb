#!/bin/bash

# --------------------------------
# Functions for colors and messages
# --------------------------------
setup_colors() {
  RED="\033[0;31m"    # Define red color code
  GREEN="\033[0;32m"  # Define green color code
  YELLOW="\033[0;33m" # Define yellow color code
  NC="\033[0m"        # No Color code to reset colors
  INFO="${GREEN}INFO${NC}    "    # Prefix for info messages
  WARNING="${YELLOW}WARNING${NC} " # Prefix for warning messages
  ERROR="${RED}$(bold ERROR)${NC}   " # Prefix for error messages using bold function
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

# --------------------------------
# Functions for script initialization
# --------------------------------
initialize_variables() {
  # Image tag to use, 'latest' by default:
  vtag="latest"
  # Command to use to run unit tests:
  unittest="python3 -m pip install 'plantdb/src/commons/.[io,test]' && nose2 -v -s plantdb/tests/"
  # Command to execute after starting the docker container:
  cmd=''
  # Volume mounting options:
  mount_option=""
  # Port flag to track if port was manually specified
  port_specified=0
  # Self-test flag (0/1 to indicate call to unittest)
  self_test=0

  # If the `ROMI_DB` variable is set, use it as default database location, else set it to empty:
  if [ -z ${ROMI_DB+x} ]; then
    host_db=''
  else
    host_db=${ROMI_DB}
  fi
}

# --------------------------------
# Usage information function
# --------------------------------
show_usage() {
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
  echo "  -p, --port
    Port to use for the REST API (default: first available port in 5000-5100 range)."
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
      ;;
    -v | --volume)
      shift
      mount_option="${mount_option} -v $1" # append
      ;;
    -p | --port)
      shift
      port=$1
      port_specified=1
      ;;
    -h | --help)
      show_usage
      exit 0
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
# Database setup functions
# --------------------------------
check_database_environment() {
  if [ -z ${ROMI_DB+x} ] && [ ${self_test} -eq 0 ]; then
    log_warning "Environment variable 'ROMI_DB' is not defined, set it to use as default database location!"
  fi
}

setup_database_mount() {
  if [ "${host_db}" != "" ]; then
    mount_option="${mount_option} -v ${host_db}:/myapp/db"
    log_info "Automatic bind mount of '${host_db}' (host) to '/myapp/db' (container)!"
  else
    # Only raise ERROR message if not a SELF-TEST:
    if [ ${self_test} -eq 0 ]; then
      log_error "No local host database defined!"
      log_info "Set 'ROMI_DB' or use the '-db' | '--database' option to define it."
      exit 1
    fi
  fi
}

setup_user_group() {
  if [ "${host_db}" != "" ]; then
    group_name=$(stat -c "%G" ${host_db})                              # get the name of the group for the 'host database path'
    gid=$(getent group ${group_name} | cut --delimiter ':' --fields 3) # get the 'gid' of this group
    log_info "Using host database path group name '${group_name}' & '${gid}'."
  else
    group_name='romi'
    gid=2020
    # Only raise WARNING message if not a SELF-TEST:
    if [ ${self_test} -eq 0 ]; then
      log_warning "Using default group name '${group_name}' & '${gid}'."
    fi
  fi
}

# --------------------------------
# Check if netstat is available
# --------------------------------
check_netstat_installed() {
  if ! command -v netstat &> /dev/null; then
    log_warning "netstat command not found. This is required to find available ports."
    log_info "To install netstat, run: sudo apt-get update && sudo apt-get install -y net-tools"
    log_info "Continuing with default port selection..."
    return 1
  fi
  return 0
}

# --------------------------------
# Port finder function
# --------------------------------
find_available_port() {
  if [ ${port_specified} -eq 0 ]; then
    log_info "Finding available port in range 5000-5100..."

    # Check if netstat is installed first
    check_netstat_installed
    netstat_available=$?

    if [ ${netstat_available} -eq 0 ]; then
      for p in $(seq 5000 5100); do
        # Check if the port is available
        if ! netstat -tuln | grep -q ":$p "; then
          port=$p
          log_info "Using available port: ${port}"
          break
        fi
      done
    else
      # If netstat isn't available, default to port 5000
      port=5000
      log_warning "Defaulting to port ${port} without availability check."
    fi

    # If no port was found, default to 5000 but warn the user
    if [ -z "${port}" ]; then
      port=5000
      log_warning "No available ports found in range 5000-5100, defaulting to ${port}. This might fail if the port is in use."
    fi
  else
    log_info "Using specified port: ${port}"
  fi
}

# --------------------------------
# Terminal handling function
# --------------------------------
check_terminal() {
  if [ -t 1 ]; then
    USE_TTY="-t"
  else
    USE_TTY=""
  fi
}

# --------------------------------
# Docker run functions
# --------------------------------
run_docker_development() {
  # Start in interactive mode, using the `-i` flag (load `~/.bashrc`).
  docker run \
    --rm \
    -p ${port}:5000 \
    ${mount_option} \
    --user romi:${gid} \
    -i ${USE_TTY} \
    roboticsmicrofarms/plantdb:${vtag} \
    "fsdb_rest_api --port 5000"
}

run_docker_production() {
  # Start the Docker container in detached mode.
  # Map the specified host port to the container's port 5000.
  # Mount options for the container filesystem.
  # Set the user and group ID within the container.
  # Load `~/.bashrc` if `-i` is set, interactive mode.
  # Use the specified Docker image tag for the roboticsmicrofarms/plantdb service.
  # Run uWSGI to serve the application on port 5000.
  docker run \
    -d \
    -p ${port}:5000 \
    ${mount_option} \
    --user romi:${gid} \
    -i ${USE_TTY} \
    roboticsmicrofarms/plantdb:${vtag} \
    "uwsgi --http :5000 --module plantdb.server.cli.wsgi:application --callable application --master"
}

run_docker_command() {
  log_info "Running: '${cmd}'."
  log_info "Bind mount: '${mount_option}'."

  # Get the date to estimate command execution time:
  start_time=$(date +%s)

  # Start in interactive mode, using the `-i` flag (load `~/.bashrc`).
  docker run --rm -p ${port}:5000 ${mount_option} \
    --user romi:${gid} \
    -i ${USE_TTY} roboticsmicrofarms/plantdb:${vtag} \
    "${cmd}"

  # Get command exit code:
  cmd_status=$?

  # Print build time if successful (code 0), else print command exit code
  if [ ${cmd_status} -eq 0 ]; then
    log_info "Command SUCCEEDED in $(($(date +%s) - start_time))s!"
  else
    log_error "Command FAILED after $(($(date +%s) - start_time))s with code ${cmd_status}!"
  fi

  # Exit with status code:
  exit ${cmd_status}
}

# --------------------------------
# Main script execution
# --------------------------------
main() {
  setup_colors
  initialize_variables
  parse_arguments "$@"
  check_database_environment
  setup_database_mount
  setup_user_group
  find_available_port
  check_terminal

  if [ "${cmd}" = "" ]; then
    run_docker_development
  else
    run_docker_command
  fi
}

# Execute main function with all arguments
main "$@"