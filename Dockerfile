# Build a static binary, then copy it into a small image.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/flotilla ./cmd/flotilla

FROM alpine:3.20
RUN adduser -D -u 10001 flotilla && mkdir /data && chown flotilla /data
COPY --from=build /out/flotilla /usr/local/bin/flotilla
USER flotilla
VOLUME /data
# Raft between nodes, HTTP for clients.
EXPOSE 7000 8000
ENTRYPOINT ["flotilla"]
