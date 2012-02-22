#!/bin/bash

basedir=`dirname $0`

export CASSANDRA_CONF=$basedir/conf/

if [ ! $CASS_HOME ]; then
    CASS_HOME="/opt/cassandra"
fi


rm -rf /tmp/cass/*
exec $CASS_HOME/bin/cassandra -f
