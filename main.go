package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	_ "github.com/jackc/pgx/v4/stdlib"
	"golang.org/x/sync/errgroup"
)

// CLI is the program input taken from the command line. It is annotated with
// struct tags for github.com/alecthomas/kong to parse.
type CLI struct {
	Input    *os.File `arg:"" help:"Input CSV filename"`
	DBUrl    string   `short:"u" help:"Database connect string URL (overrides individual options)"`
	DBName   string   `short:"d" help:"Database name" env:"PGDATABASE" default:"homework"`
	Host     string   `short:"h" help:"Database host name" env:"PGHOST" default:"localhost"`
	Port     uint16   `short:"p" help:"Database TCP port" env:"PGPORT" default:"5432"`
	Username string   `short:"U" help:"Database username" env:"PGUSER" default:"postgres"`
	Password string   `short:"p" help:"Database user password" env:"PGPASSWORD"`
}

// query is a single parsed query from the input CSV file.
type query struct {
	hostname   string
	start, end time.Time
}

// queryResult is the result of executing a query against the database.
type queryResult struct {
	// minCPU and maxCPU is the minimum and maximum CPU time for a host
	// within the start and end time of a query.
	minCPU, maxCPU float64

	// queryDuration is the amount of time it took to execute the query
	// against the database and retrieve the result.
	queryDuration time.Duration
}

type querySummary struct {
	count  int
	sum    time.Duration
	min    time.Duration
	max    time.Duration
	mean   time.Duration
	median time.Duration
}

func main() {
	cli := &CLI{}
	kong.Parse(cli)
	defer cli.Input.Close()

	db, err := dbconnect(cli)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	summary, err := run(cli, db)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Number of queries: %d\n", summary.count)
	fmt.Printf("Total processing time: %v\n", summary.sum.Truncate(time.Microsecond))
	fmt.Printf("Min / max processing time: %v / %v\n", summary.min.Truncate(time.Microsecond), summary.max.Truncate(time.Microsecond))
	fmt.Printf("Mean / median processing time: %v / %v\n", summary.mean.Truncate(time.Microsecond), summary.median.Truncate(time.Microsecond))

	os.Exit(0)
}

func dbconnect(config *CLI) (*sql.DB, error) {
	url := config.DBUrl
	if url == "" {
		format := "postgres://%s%s@%s:%d/%s"
		password := ""
		if config.Password != "" {
			password = ":" + config.Password
		}
		url = fmt.Sprintf(format, config.Username, password, config.Host, config.Port, config.DBName)
		if config.Host == "localhost" {
			url += "?sslmode=disable"
		}
	}
	return sql.Open("pgx", url)
}

// run executes the tsbench data pipeline and returns the result. Currently
// that result is just a count of input queries. As the program evolves, it
// will be the result of the benchmark.
func run(config *CLI, db *sql.DB) (querySummary, error) {
	group, ctx := errgroup.WithContext(context.Background())
	queries := make(chan query)
	queryResults := make(chan queryResult)

	var summary querySummary
	group.Go(func() error { return readQueries(ctx, config.Input, queries) })
	group.Go(func() error { return executeQueries(ctx, db, queries, queryResults) })
	group.Go(func() error {
		var err error
		summary, err = summariseResults(ctx, queryResults)
		return err
	})

	return summary, group.Wait()
}

// readQueries reads a CSV file of queries from input and sends each of them in
// order to the output channel. If the file is malformed, an error is returned,
// but not before sending any valid queries on the output channel.
//
// A well-formed CSV file has a header and each row with three columns:
//   hostname: a string
//   start_time: a time in the form YYYY-MM-DD HH:MM:SS
//   end_time: a time in the form YYYY-MM-DD HH:MM:SS
// The start and end time are in UTC.
func readQueries(ctx context.Context, input io.Reader, output chan<- query) error {
	defer close(output)

	r := csv.NewReader(input)
	header, err := r.Read()
	if err != nil {
		return err
	}
	if len(header) != 3 || header[0] != "hostname" || header[1] != "start_time" || header[2] != "end_time" {
		return fmt.Errorf("Unknown input format: %s", strings.Join(header, ", "))
	}

	for line := 1; ; line++ {
		row, err := r.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		q, err := newQuery(row)
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if !sendQuery(ctx, q, output) {
			return nil
		}
	}
}

// newQuery returns a query struct from a CSV row. It is expected that the input
// slice has 3 elements. If any of the fields are invalid, an error is returned.
func newQuery(row []string) (query, error) {
	if row[0] == "" {
		return query{}, errors.New("empty hostname")
	}
	start, err := time.Parse("2006-01-02 15:04:05", row[1])
	if err != nil {
		return query{}, fmt.Errorf("invalid start time: %s: %w", row[1], err)
	}
	end, err := time.Parse("2006-01-02 15:04:05", row[2])
	if err != nil {
		return query{}, fmt.Errorf("invalid start time: %s: %w", row[2], err)
	}

	return query{hostname: row[0], start: start, end: end}, nil
}

func executeQueries(ctx context.Context, db *sql.DB, input <-chan query, output chan<- queryResult) error {
	defer close(output)

	sqlQ := "SELECT min(usage), max(usage) FROM cpu_usage WHERE host = $1 AND ts >= $2 AND ts <= $3"
	stmt, err := db.PrepareContext(ctx, sqlQ)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var q query
	for recvQuery(ctx, &q, input) {
		qr, err := executeQuery(stmt, q)
		if err != nil {
			return err
		}
		if !sendQueryResult(ctx, qr, output) {
			return nil
		}
	}

	return nil
}

func executeQuery(stmt *sql.Stmt, q query) (queryResult, error) {
	var qr queryResult
	qStart := time.Now()

	row := stmt.QueryRow(q.hostname, q.start, q.end)
	if err := row.Scan(&qr.minCPU, &qr.maxCPU); err != nil {
		return queryResult{}, err
	}

	qr.queryDuration = time.Since(qStart)
	return qr, nil
}

// summariseResults tallies all the query results on the input channel and
// returns out a summary including the number of queries, total processing
// tme and the min, max, mean and median processing time.
func summariseResults(ctx context.Context, input <-chan queryResult) (querySummary, error) {
	summary := querySummary{}
	results := []queryResult{}

	var qr queryResult
	for recvQueryResult(ctx, &qr, input) {
		results = append(results, qr)
		summary.count++
		if qr.queryDuration < summary.min || summary.min == 0 {
			summary.min = qr.queryDuration
		}
		if qr.queryDuration > summary.max {
			summary.max = qr.queryDuration
		}
		summary.sum += qr.queryDuration
	}

	summary.mean = time.Duration(int64(summary.sum) / int64(summary.count))
	summary.median = calculateMedian(results)

	return summary, nil
}

func calculateMedian(results []queryResult) time.Duration {
	sort.Slice(results, func(i, j int) bool {
		return results[i].queryDuration < results[j].queryDuration
	})
	count := len(results)
	if count%2 == 0 {
		return (results[(count/2)-1].queryDuration + results[count/2].queryDuration) / 2
	}
	return results[count/2].queryDuration
}
