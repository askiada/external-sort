.PHONY: help
help: ## Show this
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

tag=v1.0.0
docker_image=askiada/external-sort

include ./env.list
export $(shell sed 's/=.*//' ./env.list)

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

.PHONY: build_docker
build_docker: ## Build a docker image from current git sha
	@docker build \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		-t $(docker_image):$(tag) .