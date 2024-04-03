#!/usr/bin/env bash

rm -rf web-frontend/dbi
cp -r dbi web-frontend/dbi
rm -rf script_workers/dbi
cp -r dbi script_workers/dbi

docker-compose build && docker-compose up
