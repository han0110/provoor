package openvm

import (
	"errors"
	"fmt"

	"github.com/han0110/provoor/pkg/ereverifier"
)

// verifyProof cryptographically verifies a proof envelope against the
// program's verification baseline and returns the public values it proves,
// the only ones a caller may trust. A malformed envelope and a well-formed
// one that fails verification are reported apart, so a coordinator serving
// garbage reads differently from a proof that does not hold.
func verifyProof(verifier *ereverifier.Verifier, proof []byte) ([]byte, error) {
	publicValues, err := verifier.Verify(proof)
	switch {
	case errors.Is(err, ereverifier.ErrDecodeProof):
		return nil, fmt.Errorf("decoding the proof envelope: %w", err)
	case errors.Is(err, ereverifier.ErrVerify):
		return nil, fmt.Errorf("verifying the proof: %w", err)
	case err != nil:
		return nil, fmt.Errorf("running the verifier: %w", err)
	}
	return publicValues, nil
}
