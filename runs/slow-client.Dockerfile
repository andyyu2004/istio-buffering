# Builds runs/slow_client.go into a tiny image. Built locally and loaded
# into the kind cluster via `kind load docker-image` (no registry needed).
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY slow_client.go .
ENV CGO_ENABLED=0
RUN go build -ldflags='-s -w' -o /out/slow_client slow_client.go

FROM alpine:3.19
COPY --from=build /out/slow_client /usr/local/bin/slow_client
ENTRYPOINT ["/usr/local/bin/slow_client"]
