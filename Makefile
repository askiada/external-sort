.PHONY: test
test:
	go test ./...

.PHONY: test_race
test_race:
	go test -race ./...

.PHONY: run
run:
	go run main.go