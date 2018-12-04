#!/bin/sh

if [ `id -u` = 0 ]
then
	# Perform tasks that need to be run as root

	# Re-exec this script as the application user.
	this=`readlink -f $0`
	exec su mdbc -c  "$this"
fi

if [ -z "${TABLE_CONFIG_PATH}" ]
then
	export TABLE_CONFIG_PATH=$PWD/config/tableConfiguration.json
fi

if [ -z "${CONFIG_BASE}" ]
then
	export CONFIG_BASE=config
fi

if [ -z "$AVATICA_PORT" ]
then
	AVATICA_PORT=30000
fi

if [ -z "$JDBC_URL" ]
then
	echo "JDBC_URL environment variable is not set" 1>&2
	exit 1
fi

if [ -z "$JDBC_USER" ]
then
	echo "JDBC_USER environment variable is not set" 1>&2
	exit 1
fi

if [ -z "$JDBC_PASSWORD" ]
then
	echo "JDBC_PASSWORD environment variable is not set" 1>&2
	exit 1
fi

jvmargs="${JVM_ARGS}"

echo "JVM Arguments: ${jvmargs}"

if [ ! -s ${CONFIG_BASE}-0.json ]
then
	echo "Running CreateNodeConfigurations"

	java ${jvmargs} -cp config:mdbc-server.jar org.onap.music.mdbc.tools.CreateNodeConfigurations -t ${TABLE_CONFIG_PATH} -b ${CONFIG_BASE} -o $PWD

	if [[ $? != 0 ]]
	then
		echo "CreateNodeConfigurations failed"
		exit 1
	fi

	if [ ! -s ${CONFIG_BASE}-0.json ]
	then
		echo "Configuration not created correctlly: ${CONFIG_BASE}-0.json"
		exit 1
	fi

	echo "CreateNodeConfigurations created ${CONFIG_BASE}-0.json"
fi

echo "Running MdbcServer"

java ${jvmargs} -cp config:mdbc-server.jar org.onap.music.mdbc.MdbcServer -c ${CONFIG_BASE}-0.json -p ${AVATICA_PORT} -u ${JDBC_URL} -s ${JDBC_USER} -a ${JDBC_PASSWORD}
rc=$?

echo "Application exiting with status code $rc"

if [ ! -z "${EXIT_DELAY}" -a "${EXIT_DELAY}" != 0 ]; then
	echo "Delaying exit for $EXIT_DELAY seconds"
	sleep $EXIT_DELAY
fi

exit $rc
