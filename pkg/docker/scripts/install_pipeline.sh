#!/bin/bash

apt-get update
apt-get -f install

# install some basics
apt-get install -y wget nano

# install dependencies
apt-get install -y sudo

wget -q http://www.pipelinedb.com/download/0.9.6/debian8

dpkg --install debian8

useradd -m pipeline -s "/bin/bash"
