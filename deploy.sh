#!/bin/bash
set -e

echo "Creating data directories..."
mkdir -p ~/lastfm-analyzer-data
mkdir -p ~/lastfm-analyzer-data/input
mkdir -p ~/lastfm-analyzer-data/output

echo "Building Docker image..."
docker build -t lastfm-analyzer .

echo "Downloading data files..."
wget -O - http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz | tar xz -C ~/lastfm-analyzer-data/input

echo "Starting Spark Docker container and running analysis..."
docker run --rm\
  --name lastfm-analyzer \
  -v ~/lastfm-analyzer-data:/data \
  -e INPUT_PATH='/data/input/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv' \
  -e OUTPUT_PATH='/data/output/top-songs.tsv' \
  lastfm-analyzer

echo "Output:"
cat ~/lastfm-analyzer-data/output/top-songs.tsv
