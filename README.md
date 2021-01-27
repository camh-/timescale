# tsbench

A timescale DB benchmarking tool.

## Building

    make build

will build the tool writing it to the `out/` directory. `make install`
will install it in `$GOBIN` (`$GOPATH/bin`).

    make

will build, test, lint and check test coverage.

## Running the database

    make start-db

will start a TimescaleDB database in a docker container, with port 5432
mapped to the host network. It is created without a root password. All
data is stored ephemerally, and will be lost when the container is
stopped.

The database is started and the initial schema from
`schema/cpu_usage.sql` is created. The data in `testdata/cpu_usage.csv`
is loaded into the database.

    make stop-db

will stop the database container.

## Running the benchmark

    ./out/tsbench testdata/query_params.csv

will run the benchmark with the queries in the file specified on the
command line.
