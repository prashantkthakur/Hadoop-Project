#!/bin/bash

export YARN_NODEMANAGER_OPTS="-Dhadoop.tmp.dir=/s/${HOSTNAME}/a/tmp/hadoop-${USER}"

