#!/bin/sh

DATA_DIR="./data"

# Check if the directory exists
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
fi

# Run Docker Compose
docker compose up --build