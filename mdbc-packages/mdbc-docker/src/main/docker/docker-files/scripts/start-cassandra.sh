#!/bin/bash

# This enhances the standard docker entrypoint script by running cql scripts
# and shell scripts present in /docker-entrypoint-initdb.d. The public native
# transport port is not exposed until all the scripts have been completed.

if [[ $PRIVATE_PORT == "" ]]
then
	PRIVATE_PORT=19042
fi

if [[ $START_TIMEOUT_SECS == "" ]]
then
	START_TIMEOUT_SECS=300
fi

function getPort
{
	grep "^native_transport_port:" /etc/cassandra/cassandra.yaml | cut -d: -f2 | tr -d '\t '
}

function setPort
{
	local port=$1
	sed -i "s/^native_transport_port:.*/native_transport_port: $port/" /etc/cassandra/cassandra.yaml
}

function usage
{
	echo "usage: $(basename $0) [-sX]"
}

sleep=0
args=

while [[ $# != 0 ]]
do
	case "$1" in
	-s*)
		sleep=${1:2}
		shift 1
		;;
	*)
		usage && exit 1
		;;
	esac
done

if [[ $# != 0 ]]
then
	usage && exit 1
fi

if [[ $sleep != 0 ]]
then
	echo "Delaying startup by $sleep seconds"
	sleep $sleep
fi

# Note: this does not start up cassandra
echo "Running docker-entrypoint.sh to configure cassandra"
/docker-entrypoint.sh true || exit 1

echo "Enabling password authentication"
sed -i 's/^authenticator: AllowAllAuthenticator/authenticator: PasswordAuthenticator/g' /etc/cassandra/cassandra.yaml || exit 1

if [[ $(/bin/ls -1 /docker-entrypoint-initdb.d 2>/dev/null | wc -l) == 0 ]]
then
	echo "Starting cassandra"
else
	# Get the public native transport port from cassandra.yaml

	public_port=$(getPort)

	if [[ $public_port == "" ]]
	then
		echo "ERROR: No native_transport_port in /etc/cassandra/cassandra.yaml"
		exit 1
	fi

	echo "Starting cassandra on port $PRIVATE_PORT"
	setPort $PRIVATE_PORT || exit 1

	/docker-entrypoint.sh cassandra

	if [[ $? != 0 ]]
	then
		setPort $public_port
		exit 1
	fi

	tries=$START_TIMEOUT_SECS

	while true
	do
		if echo "describe keyspaces;" | cqlsh -u cassandra -p cassandra 127.0.0.1 $PRIVATE_PORT >/dev/null 2>&1
		then
			break
		fi

		tries=$((tries-1))

		if [[ $tries == 0 ]]
		then
			setPort $public_port
			echo "Timed out waiting for cassandra to start on port $PRIVATE_PORT"
			exit 1
		fi

		sleep 1
	done

	echo "Cassandra is started on port $PRIVATE_PORT"

	for file in $(/bin/ls -1 /docker-entrypoint-initdb.d 2>/dev/null)
	do
		case "$file" in
		*.sh)
			echo "Running $file"
			source "/docker-entrypoint-initdb.d/$file"
			;;
		*.cql)
			echo "Running $file"
			cqlsh -u cassandra -p cassandra -f "/docker-entrypoint-initdb.d/$file" 127.0.0.1 $PRIVATE_PORT
			;;
        	*)
			echo "Ignoring $file"
    		esac
	done

	sleep 5

	echo "Restarting cassandra on port $public_port"

	setPort $public_port
	pkill java

	if [[ $? != 0 ]]
	then
		echo "Failed to restart cassandra (kill failed)"
		exit 1
	fi

	sleep 5
fi

# NOTE: this starts cassandra in the foreground
/docker-entrypoint.sh cassandra -f
