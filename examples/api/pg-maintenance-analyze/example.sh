#!/bin/sh

export DB_CONNECTION_STRING=postgres://postgres@localhost:5433/postgres
export LISTEN_ADDR=127.0.0.1:7308

./pg-maintenance-analyze
