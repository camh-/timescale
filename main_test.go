package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// test fixtures
var (
	goodHeader   = "hostname,start_time,end_time\n"
	good1        = "host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22\n"
	good2        = "host_000001,2017-01-02 13:02:02,2017-01-02 14:02:02\n"
	badHeader    = "hostname,start_time\n"
	badHostname  = ",2017-01-01 08:59:22,2017-01-01 09:59:22\n"
	badStartTime = "host_000008,08:59:22 2017-01-01,2017-01-01 09:59:22\n"
	badEndTime   = "host_000008,2017-01-01 08:59:22,2017-21-01 09:59:22\n"
	badRow       = "hostname,\n"
	emptyRow     = "\n"

	good1Query = query{
		hostname: "host_000008",
		start:    mustParseTime("2017-01-01T08:59:22Z"),
		end:      mustParseTime("2017-01-01T09:59:22Z"),
	}
	good2Query = query{
		hostname: "host_000001",
		start:    mustParseTime("2017-01-02T13:02:02Z"),
		end:      mustParseTime("2017-01-02T14:02:02Z"),
	}
)

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// collect receives the queries on the input channel and returns them all in a slice.
func collect(input <-chan query) []query {
	result := []query{}
	for q := range input {
		result = append(result, q)
	}
	return result
}

// parse is a helper function that calls readQueries and collects the results
// in a slice.
func parse(input string) ([]query, error) {
	queries := make(chan query)
	var err error
	go func() { err = readQueries(context.Background(), strings.NewReader(input), queries) }()
	got := collect(queries)
	return got, err
}

func TestReadQueries(t *testing.T) {
	want := []query{good1Query, good2Query}
	got, err := parse(goodHeader + good1 + good2)
	require.NoError(t, err)
	require.Equal(t, want, got)

	got, err = parse(goodHeader + good1 + emptyRow + good2)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = parse(goodHeader + badRow)
	require.Error(t, err)

	_, err = parse(goodHeader + badHostname)
	require.Error(t, err)

	_, err = parse(goodHeader + badStartTime)
	require.Error(t, err)

	_, err = parse(goodHeader + badEndTime)
	require.Error(t, err)

	_, err = parse(badHeader + good1)
	require.Error(t, err)
}
