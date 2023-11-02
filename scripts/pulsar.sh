#!/bin/bash

cr='docker'
PODMAN_EXISTS=$(which podman)
RET_VAL=$?

if [ $RET_VAL -eq '0' ]; then
    echo "podman exists."
    cr='podman'
else 
    echo "podman does not exist. using docker"
fi

$cr run --name=pulsar -d -p 6650:6650  -p 8081:8080 docker.io/apachepulsar/pulsar:2.11.0 bin/pulsar standalone
