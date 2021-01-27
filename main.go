package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"golang.org/x/sync/errgroup"
)

// CLI is the program input taken from the command line. It is annotated with
// struct tags for github.com/alecthomas/kong to parse.
type CLI struct {
	Input *os.File `arg:"" help:"Input CSV filename"`
}

// query is a single parsed query from the input CSV file.
type query struct {
	hostname   string
	start, end time.Time
}

func main() {
	cli := &CLI{}
	kong.Parse(cli)
	defer cli.Input.Close()

	count, err := run(cli)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("%d records read\n", count)
	os.Exit(0)
}

// run executes the tsbench data pipeline and returns the result. Currently
// that result is just a count of input queries. As the program evolves, it
// will be the result of the benchmark.
func run(config *CLI) (int, error) {
	group := &errgroup.Group{}
	queries := make(chan query)

	count := 0
	group.Go(func() error { return readQueries(config.Input, queries) })
	group.Go(func() error {
		var err error
		count, err = countQueries(queries)
		return err
	})

	return count, group.Wait()
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
func readQueries(input io.Reader, output chan<- query) error {
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
		output <- q
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

// countQueries is a temporary function to be the final consumer in the data
// pipeline. It will be replaced as the program evolves.
func countQueries(input <-chan query) (int, error) {
	count := 0
	for range input {
		count++
	}
	return count, nil
}
