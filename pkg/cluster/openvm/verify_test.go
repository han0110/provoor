package openvm

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/han0110/provoor/pkg/ereverifier"
)

// The fixtures are a real proof of the warmup block, produced by a cluster
// running the stateless-validator-reth-openvm-v2.1.0-preview guest of
// ere-guests v0.15.0, together with the verification baseline the coordinator
// serves for it and the public values it proves. other-baseline.bin is the
// baseline of the ethrex guest of the same release, a program the proof does
// not attest to.
const (
	proofFixture         = "testdata/proof.bin"
	baselineFixture      = "testdata/baseline.bin"
	otherBaselineFixture = "testdata/other-baseline.bin"
	publicValuesFixture  = "testdata/public-values.bin"
)

// unverifiableProof stands in for a proof envelope. The verifier rejects it
// at the decode step.
var unverifiableProof = []byte("not-an-encoded-proof")

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

// programVerifyingKeyFixture is a guest program's verification baseline, the
// bytes the coordinator serves per program name.
func programVerifyingKeyFixture(t *testing.T) []byte {
	t.Helper()
	return readFixture(t, baselineFixture)
}

func newFixtureVerifier(t *testing.T, programVerifyingKey []byte) *ereverifier.Verifier {
	t.Helper()
	verifier, err := ereverifier.New(ereverifier.OpenVM, programVerifyingKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verifier.Close)
	return verifier
}

func TestVerifyProof(t *testing.T) {
	verifier := newFixtureVerifier(t, programVerifyingKeyFixture(t))

	publicValues, err := verifyProof(verifier, readFixture(t, proofFixture))
	if err != nil {
		t.Fatalf("verifyProof() = %v, want a verified proof", err)
	}
	if want := readFixture(t, publicValuesFixture); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

// TestVerifyProofRejectsAnotherProgram covers the binding that makes a proof
// evidence about one guest rather than about any program the cluster chose to
// run, since the baseline enters the verifying key the proof is checked
// against.
func TestVerifyProofRejectsAnotherProgram(t *testing.T) {
	verifier := newFixtureVerifier(t, readFixture(t, otherBaselineFixture))

	_, err := verifyProof(verifier, readFixture(t, proofFixture))
	if !errors.Is(err, ereverifier.ErrVerify) {
		t.Errorf("err = %v, want a verification failure", err)
	}
}

func TestVerifyProofRejectsTamperedProof(t *testing.T) {
	verifier := newFixtureVerifier(t, programVerifyingKeyFixture(t))
	tampered := readFixture(t, proofFixture)
	tampered[len(tampered)/2] ^= 1

	_, err := verifyProof(verifier, tampered)
	if err == nil {
		t.Fatal("verifyProof() = nil, want a tampered proof to be rejected")
	}
	if !errors.Is(err, ereverifier.ErrVerify) && !errors.Is(err, ereverifier.ErrDecodeProof) {
		t.Errorf("err = %v, want a verification or decoding failure", err)
	}
}

func TestVerifyProofRejectsUndecodableProof(t *testing.T) {
	verifier := newFixtureVerifier(t, programVerifyingKeyFixture(t))

	_, err := verifyProof(verifier, unverifiableProof)
	if !errors.Is(err, ereverifier.ErrDecodeProof) || !strings.Contains(err.Error(), "decoding the proof envelope") {
		t.Errorf("err = %v, want a proof decoding failure", err)
	}
}
