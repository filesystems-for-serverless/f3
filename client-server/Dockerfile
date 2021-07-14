FROM golang:1.16.5-alpine3.14 as build

RUN mkdir /app

WORKDIR /app
ADD src/client .
ADD src/server .

RUN go mod init main
RUN go mod tidy
RUN go build client.go
RUN go build server.go

FROM alpine:3.14
COPY --from=build /app /app

CMD ["-socket-file", "fuse-client.sock", "-temp-dir", "/mnt/local-cache/client_tempdr/", "-threshold-requests", "10"]
ENTRYPOINT [ "/app/client" ]
