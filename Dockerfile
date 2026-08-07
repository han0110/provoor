FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 go build -o /provoor ./cmd/provoor

FROM gcr.io/distroless/static-debian12
COPY --from=build /provoor /usr/local/bin/provoor
ENTRYPOINT ["provoor"]
