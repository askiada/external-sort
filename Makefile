.PHONY: test
test:
	go test ./...

.PHONY: test_race
test_race:
	go test -race ./...

.PHONY: run
run: build
	./bin/external-sort

.PHONY: build
build:
	go build -o bin/external-sort main.go