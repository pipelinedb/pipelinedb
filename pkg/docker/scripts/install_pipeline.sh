#!/bin/bash

apt-get update
apt-get -f install

# install some basics
apt-get install -y wget nano

# install dependencies
apt-get install -y libxml2 libxml2-dev libcurl3

wget http://www.pipelinedb.com/download/0.7.7/ubuntu14

dpkg --install ubuntu14

useradd -m pipeline -s "/bin/bash"



