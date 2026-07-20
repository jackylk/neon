#!/usr/bin/env bash
set -ex
cd "$(dirname "${0}")"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
dropdb --if-exists contrib_regression
createdb contrib_regression
cd h3/test
TESTS=$(echo sql/* | sed 's|sql/||g; s|\.sql||g')
psql -d contrib_regression -c "CREATE EXTENSION h3"
${PG_REGRESS} --use-existing --dbname contrib_regression ${TESTS}
