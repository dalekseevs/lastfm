#!/bin/bash
set -e

echo "Building Docker image..."
docker build -t lastfm-analyzer .

echo "Creating Docker volume..."
docker volume create job-data

echo "Downloading data files..."
wget -O - http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz | tar xz -C /tmp/
docker run --rm --name busybox-copy-input -v job-data:/data -v /tmp/lastfm-dataset-1K:/tmp/lastfm-dataset-1K busybox cp -r /tmp/lastfm-dataset-1K /data/input

echo "Starting Spark Docker container and running analysis..."
docker run \
  --name lastfm-analyzer \
  -v job-data:/data \
  -e INPUT_PATH='/data/input/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv' \
  -e OUTPUT_PATH='/data/output/top-songs.tsv' \
  lastfm-analyzer
docker logs -f lastfm-analyzer

echo "Copying output file to local filesystem..."
docker run --rm --name busybox-copy-output -v job-data:/data busybox cp /data/output/top-songs.tsv /tmp/

echo "Cleaning up..."
docker volume rm job-data

echo "Output file:"
cat /tmp/top-songs.tsv
