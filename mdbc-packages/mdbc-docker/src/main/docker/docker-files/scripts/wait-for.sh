#!/bin/sh
# https://github.com/Eficode/wait-for.git
# MIT License
# Modified to wait for multiple ports to open

TIMEOUT=15
QUIET=0
ADDRESSES=

echoerr() {
  if [ "$QUIET" -ne 1 ]; then printf "%s\n" "$*" 1>&2; fi
}

usage() {
  exitcode="$1"
  cat << USAGE >&2
Usage:
  wait-for host:port [host:port ... ] [-t timeout] [-- command args]
  -q | --quiet                        Do not output any status messages
  -t TIMEOUT | --timeout=timeout      Timeout in seconds, zero for no timeout
  -- COMMAND ARGS                     Execute command with args after the test finishes
USAGE
  exit "$exitcode"
}

wait_for() {
  command="$*"
  if [ "$QUIET" -ne 1 ]; then echo "$0: probing host $HOST port $PORT"; fi
  for i in `seq $TIMEOUT` ; do
    ready=TRUE
    set DUMMY $ADDRESSES; shift 1
    while [ $# -gt 0 ] ; do
      host=$1
      port=$2
      shift 2
      if ! nc -z "$host" "$port" > /dev/null 2>&1 ; then
        ready=FALSE
        break
      fi
    done
    if [ $ready = TRUE ] ; then
      if [ "$QUIET" -ne 1 ] ; then
        echo "$0: operation succeeded on try $i"
      fi
      if [ -n "$command" ] ; then
        if [ "$QUIET" -ne 1 ] ;
          then echo "$0: exec-ing command $command" ;
        fi
        exec $command
      fi
      exit 0
    fi
    if [ "$QUIET" -ne 1 ] ; then
      echo "$0: sleeping after try $i" ;
    fi
    sleep 1
  done
  echo "$0: Operation timed out" >&2
  exit 1
}

while [ $# -gt 0 ]
do
  case "$1" in
    *:* )
    host=$(printf "%s\n" "$1"| cut -d : -f 1)
    port=$(printf "%s\n" "$1"| cut -d : -f 2)
    ADDRESSES="$ADDRESSES $host $port"
    shift 1
    ;;
    -q | --quiet)
    QUIET=1
    shift 1
    ;;
    -t)
    TIMEOUT="$2"
    if [ "$TIMEOUT" = "" ]; then break; fi
    shift 2
    ;;
    --timeout=*)
    TIMEOUT="${1#*=}"
    shift 1
    ;;
    --)
    shift
    break
    ;;
    --help)
    usage 0
    ;;
    *)
    echoerr "Unknown argument: $1"
    usage 1
    ;;
  esac
done

if [ "$ADDRESSES" = "" ] ; then
  echoerr "Error: you need to provide at least one host and port to test."
  usage 2
fi

wait_for "$@"
