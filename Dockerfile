FROM golang:1.24-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /provoor ./cmd/provoor

FROM gcr.io/distroless/static-debian12
COPY --from=build /provoor /usr/local/bin/provoor
ENTRYPOINT ["provoor"]
