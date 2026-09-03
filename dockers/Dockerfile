# VERIFIER_LIB selects where libere_verifier_c comes from. release downloads
# the pinned ere asset and local takes the library already in
# internal/ereverifier/lib.
ARG VERIFIER_LIB=release

FROM golang:1.24-bookworm AS verifier-release

WORKDIR /src

COPY scripts/ scripts/
RUN scripts/fetch-verifier.sh

FROM golang:1.24-bookworm AS verifier-local

WORKDIR /src
COPY internal/ereverifier/lib/ internal/ereverifier/lib/

FROM verifier-${VERIFIER_LIB} AS verifier

FROM golang:1.24-bookworm AS build

ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/
# COPY merges into an existing directory, so a library the context carries is
# removed before the selected one lands.
RUN rm -rf internal/ereverifier/lib
COPY --from=verifier /src/internal/ereverifier/lib/ internal/ereverifier/lib/

RUN CGO_ENABLED=1 go build -ldflags "-X main.version=${VERSION}" -o /provoor ./cmd/provoor

# The verifier is a Rust static library, so the binary needs the glibc and
# libgcc unwinder that the cc image carries.
FROM gcr.io/distroless/cc-debian12

COPY --from=build /provoor /usr/local/bin/provoor

ENTRYPOINT ["provoor"]
