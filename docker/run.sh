#!/bin/bash

user=$USER
host_db="/data/ROMI/DB"
vtag="latest"
unittest_cmd="nosetests plantdb/tests/ --with-coverage --cover-package=plantdb"

usage() {
  echo "USAGE:"
  echo "  ./run.sh [OPTIONS]
    "

  echo "DESCRIPTION:"
  echo "  Run 'roboticsmicrofarms/plantdb:<vtag>' container with a mounted local (host) database and expose it to port 5000.
    "

  echo "OPTIONS:"
  echo "  -t, --tag
    Docker image tag to use, default to '$vtag'.
    "
  echo "  -p, --database_path
    Path to the host database to mount inside docker container, default to '$host_db'.
    "
  echo " --unittest_cmd
    Runs unit tests defined in plantdb.
    "
  echo "  -u, --user
    User used during docker image build, default to '$user'.
    "
  echo "  -h, --help
    Output a usage message and exit.
    "
}

while [ "$1" != "" ]; do
  case $1 in
  -t | --tag)
    shift
    vtag=$1
    ;;
  -u | --user)
    shift
    user=$1
    ;;
  -p | --database_path)
    shift
    host_db=$1
    ;;
  --unittest_cmd)
    cmd=$unittest_cmd
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

if [ "$cmd" = "" ]
then
  docker run -it -p 5000:5000 \
    -v $host_db:/home/$user/db \
    roboticsmicrofarms/plantdb:$vtag
else
  docker run -it -p 5000:5000 \
    -v $host_db:/home/$user/db \
    roboticsmicrofarms/plantdb:$vtag \
    bash -c "$cmd"
fi