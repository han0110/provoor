FROM golang:1.24-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY scripts/ scripts/
RUN scripts/fetch-verifier.sh
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=1 go build -ldflags "-X main.version=${VERSION}" -o /provoor ./cmd/provoor

# The verifier is a Rust static library, so the binary needs glibc and the
# unwinder in libgcc, which is what the cc image carries and the base one
# does not.
FROM gcr.io/distroless/cc-debian12
COPY --from=build /provoor /usr/local/bin/provoor
ENTRYPOINT ["provoor"]
