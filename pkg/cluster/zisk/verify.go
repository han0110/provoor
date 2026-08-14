package zisk

import (
	"errors"
	"fmt"

	"github.com/han0110/provoor/pkg/ereverifier"
)

// verifyProof transcodes a proof envelope into the encoding ere's verifier
// accepts and verifies it against the bound program verifying key. The public
// values it returns are the verifier's, the only ones the proof attests to.
// The transcode rejects a malformed envelope on its own, so a decode failure
// here means the transcode emitted something the verifier cannot read.
func verifyProof(verifier *ereverifier.Verifier, envelope []byte) ([]byte, error) {
	proof, err := transcodeProof(envelope)
	if err != nil {
		return nil, err
	}
	publicValues, err := verifier.Verify(proof)
	switch {
	case errors.Is(err, ereverifier.ErrDecodeProof):
		return nil, fmt.Errorf("decoding the transcoded proof: %w", err)
	case errors.Is(err, ereverifier.ErrVerify):
		return nil, fmt.Errorf("verifying the proof: %w", err)
	case err != nil:
		return nil, fmt.Errorf("running the verifier: %w", err)
	}
	return publicValues, nil
}
