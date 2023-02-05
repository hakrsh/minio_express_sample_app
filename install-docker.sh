#!/bin/sh

set -o errexit
set -o nounset

IFS=$'\n\r'

# Docker
if [ -x "$(command -v docker)" ]; then
    echo "Docker is already installed"
else
    echo "Installing Docker..."
    sudo apt update
    sudo apt --yes --no-install-recommends install apt-transport-https ca-certificates curl software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
    sudo apt update
    sudo apt --yes --no-install-recommends install docker-ce
    sudo usermod --append --groups docker "$USER"
    sudo systemctl enable docker
    echo "Docker installed successfully"
fi
# Docker Compose
if [ ! -f /usr/local/bin/docker-compose ]; then
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    echo "Docker Compose installed successfully"
else
    echo "Docker Compose is already installed"
fi

