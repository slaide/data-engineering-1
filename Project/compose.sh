#!/usr/bin/env bash

rm -rf dbi/build

rm -rf web-frontend/dbi
cp -r dbi web-frontend/dbi
rm -rf script_workers/dbi
cp -r dbi script_workers/dbi

rm -rf script_workers/cell-profile
cp -r cell-profile script_workers/cell-profile

docker-compose build && docker-compose up
