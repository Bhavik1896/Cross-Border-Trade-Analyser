#!/bin/sh
# wait-for-db.sh

set -e

# Wait for TimescaleDB to be available
host="db"
port="5432"
cmd="$@"

until PGPASSWORD=password psql -h "$host" -U "user" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"
exec $cmd