# --- Global -------------------------------------------------------------------
O = out
COVERAGE = 19

all: build test check-coverage lint  ## build, test, check coverage and lint
	@if [ -e .git/rebase-merge ]; then git --no-pager log -1 --pretty='%h %s'; fi
	@echo '$(COLOUR_GREEN)Success$(COLOUR_NORMAL)'

clean::  ## Remove generated files
	-rm -rf $(O)

.PHONY: all clean

# --- Build --------------------------------------------------------------------

build: | $(O)  ## Build binaries of directories in ./cmd to out/
	go build -o $(O) .

install:  ## Build and install binaries in $GOBIN or $GOPATH/bin
	go install .

.PHONY: build install

# --- Test ---------------------------------------------------------------------
COVERFILE = $(O)/coverage.txt

test: ## Run tests and generate a coverage file
	go test -coverprofile=$(COVERFILE) ./...

check-coverage: test  ## Check that test coverage meets the required level
	@go tool cover -func=$(COVERFILE) | $(CHECK_COVERAGE) || $(FAIL_COVERAGE)

cover: test  ## Show test coverage in your browser
	go tool cover -html=$(COVERFILE)

CHECK_COVERAGE = awk -F '[ \t%]+' '/^total:/ {print; if ($$3 < $(COVERAGE)) exit 1}'
FAIL_COVERAGE = { echo '$(COLOUR_RED)FAIL - Coverage below $(COVERAGE)%$(COLOUR_NORMAL)'; exit 1; }

.PHONY: check-coverage cover test

# --- Lint ---------------------------------------------------------------------
GOLINT_VERSION = 1.33.2
GOLINT_INSTALLED_VERSION = $(or $(word 4,$(shell golangci-lint --version 2>/dev/null)),0.0.0)
GOLINT_MIN_VERSION = $(shell printf '%s\n' $(GOLINT_VERSION) $(GOLINT_INSTALLED_VERSION) | sort -V | head -n 1)
GOPATH1 = $(firstword $(subst :, ,$(GOPATH)))
LINT_TARGET = $(if $(filter $(GOLINT_MIN_VERSION),$(GOLINT_VERSION)),lint-with-local,lint-with-docker)

lint: $(LINT_TARGET)  ## Lint source code

lint-with-local:  ## Lint source code with locally installed golangci-lint
	golangci-lint run

lint-with-docker:  ## Lint source code with docker image of golangci-lint
	docker run --rm -w /src \
		-v $(shell pwd):/src -v $(GOPATH1):/go -v $(HOME)/.cache:/root/.cache \
		golangci/golangci-lint:v$(GOLINT_VERSION) \
		golangci-lint run

.PHONY: lint lint-with-local lint-with-docker


# --- Database -----------------------------------------------------------------
start-db:
	./scripts/pgdocker start tsdb
	docker exec -i tsdb psql -U postgres < schema/cpu_usage.sql
	docker exec -i tsdb psql -U postgres -d homework \
		-c "\COPY cpu_usage FROM STDIN CSV HEADER" < testdata/cpu_usage.csv

stop-db:
	./scripts/pgdocker stop

.PHONY: start-db stop-db

# --- Utilities ----------------------------------------------------------------
COLOUR_NORMAL = $(shell tput sgr0 2>/dev/null)
COLOUR_RED    = $(shell tput setaf 1 2>/dev/null)
COLOUR_GREEN  = $(shell tput setaf 2 2>/dev/null)
COLOUR_WHITE  = $(shell tput setaf 7 2>/dev/null)

help:
	@awk -F ':.*## ' 'NF == 2 && $$1 ~ /^[A-Za-z0-9%_-]+$$/ { printf "$(COLOUR_WHITE)%-30s$(COLOUR_NORMAL)%s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

$(O):
	@mkdir -p $@

.PHONY: help
