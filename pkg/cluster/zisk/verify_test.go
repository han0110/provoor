package zisk

import (
	"bytes"
	"errors"
	"testing"

	"github.com/han0110/provoor/pkg/ereverifier"
)

// TestVerifyProof runs the fixture envelope through the verifier ere ships,
// the only authority on whether the transcode produces what it accepts, and
// pins that the reported public values are the proven ones.
func TestVerifyProof(t *testing.T) {
	verifier := boundToFixture(t, readFixture(t, "program_vk.bin"))

	publicValues, err := verifyProof(verifier, fixtureEnvelope(t).encode())
	if err != nil {
		t.Fatal(err)
	}
	if want := readFixture(t, "public_values.bin"); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

// TestVerifyProofClusterEnvelope verifies an envelope a coordinator really
// returned, for the warmup block proved by the guest ere-guests v0.15.0
// publishes, against that release's published verifying key. Where
// TestVerifyProof exercises the transcode against a synthesized envelope, this
// pins it against the encoding a live cluster emits.
func TestVerifyProofClusterEnvelope(t *testing.T) {
	verifier := boundToFixture(t, readFixture(t, "cluster-program-vk.bin"))

	publicValues, err := verifyProof(verifier, readFixture(t, "cluster-envelope.bin"))
	if err != nil {
		t.Fatalf("verifyProof() = %v, want a verified proof", err)
	}
	if want := readFixture(t, "cluster-public-values.bin"); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

// TestVerifyProofClusterEnvelopeRejectsAnotherProgram covers the binding to
// the guest, since a coordinator reporting a verifying key for a program it
// did not prove is what the check exists to catch.
func TestVerifyProofClusterEnvelopeRejectsAnotherProgram(t *testing.T) {
	programVK := readFixture(t, "program_vk.bin")
	verifier := boundToFixture(t, programVK)

	_, err := verifyProof(verifier, readFixture(t, "cluster-envelope.bin"))
	if !errors.Is(err, ereverifier.ErrVerify) {
		t.Errorf("err = %v, want a verification failure", err)
	}
}

func TestVerifyProofRejects(t *testing.T) {
	cases := []struct {
		name      string
		programVK func([]byte) []byte
		mutate    func(*envelope)
	}{
		{
			name:      "wrong_program_vk",
			programVK: func(vk []byte) []byte { return append([]byte{vk[0] ^ 0xff}, vk[1:]...) },
		},
		{
			name:   "corrupted_proof",
			mutate: func(e *envelope) { e.proofWords[len(e.proofWords)/2] ^= 1 },
		},
		{
			name:   "corrupted_public_values",
			mutate: func(e *envelope) { e.publicValues[0] ^= 0xff },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			programVK := readFixture(t, "program_vk.bin")
			if tc.programVK != nil {
				programVK = tc.programVK(programVK)
			}
			e := fixtureEnvelope(t)
			if tc.mutate != nil {
				tc.mutate(&e)
			}
			_, err := verifyProof(boundToFixture(t, programVK), e.encode())
			if !errors.Is(err, ereverifier.ErrVerify) {
				t.Errorf("err = %v, want %v", err, ereverifier.ErrVerify)
			}
		})
	}
}

func boundToFixture(t *testing.T, programVK []byte) *ereverifier.Verifier {
	t.Helper()
	verifier, err := ereverifier.New(ereverifier.Zisk, programVK)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verifier.Close)
	return verifier
}
