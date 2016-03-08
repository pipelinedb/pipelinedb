#!/bin/bash

apt-get update
apt-get -f install

# install some basics
apt-get install -y wget nano

# install dependencies
apt-get install -y libxml2 libxml2-dev

wget -q http://www.pipelinedb.com/download/0.8.6/ubuntu14

dpkg --install ubuntu14

useradd -m pipeline -s "/bin/bash"
