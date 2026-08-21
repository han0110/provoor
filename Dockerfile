# Where libere_verifier_c comes from. release downloads the pinned ere release
# asset, the only source whose bytes the build checks. local takes the library
# the build context already carries in pkg/ereverifier/lib, which is how an
# image is built against an ere revision that has no release yet. Each mode is
# a stage of its own, so the download depends on scripts alone and stays cached
# across source edits, and only the selected stage is ever built.
ARG VERIFIER_LIB=release

FROM golang:1.24-bookworm AS verifier-release

WORKDIR /src

COPY scripts/ scripts/
RUN scripts/fetch-verifier.sh

FROM golang:1.24-bookworm AS verifier-local

WORKDIR /src
COPY pkg/ereverifier/lib/ pkg/ereverifier/lib/

FROM verifier-${VERIFIER_LIB} AS verifier

FROM golang:1.24-bookworm AS build

ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/
# A context carrying a local library copies it in above, and COPY merges into
# an existing directory rather than replacing it, so the directory is emptied
# before the selected verifier lands in it.
RUN rm -rf pkg/ereverifier/lib
COPY --from=verifier /src/pkg/ereverifier/lib/ pkg/ereverifier/lib/

RUN CGO_ENABLED=1 go build -ldflags "-X main.version=${VERSION}" -o /provoor ./cmd/provoor

# The verifier is a Rust static library, so the binary needs glibc and the
# unwinder in libgcc, which is what the cc image carries and the base one
# does not.
FROM gcr.io/distroless/cc-debian12

COPY --from=build /provoor /usr/local/bin/provoor

ENTRYPOINT ["provoor"]
