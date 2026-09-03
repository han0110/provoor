//go:build cgo

package ereverifier

// The vendored binding names the library and its platform link dependencies
// but not where they live. scripts/fetch-verifier.sh populates lib with the
// header and the static archive of the matching ere release. The SRCDIR
// substitution resolves that directory at build time, so an ordinary go build
// needs no environment of its own.

/*
#cgo CFLAGS: -I${SRCDIR}/lib
#cgo LDFLAGS: -L${SRCDIR}/lib
*/
import "C"
