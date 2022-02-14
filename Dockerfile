FROM golang:1.17-alpine AS builder

WORKDIR /go/src/github.com/askiada/external-sort

# Caching dependencies.
COPY go.mod .
COPY go.sum .

COPY . .
RUN chmod -R 777 .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -a -installsuffix cgo \
    -ldflags="-w -s" \
    -trimpath \
    -o /bin/external-sort main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/external-sort /external-sort

USER root
ENTRYPOINT ["/external-sort"]