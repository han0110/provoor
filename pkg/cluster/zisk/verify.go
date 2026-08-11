package zisk

// verifyProof cryptographically verifies a proof envelope before its public
// values are trusted. The implementation lands with the libere_verifier_c
// library ere v0.15.0 releases, until then every proof passes and validity
// rests on the public-values comparison alone.
func verifyProof(proof []byte) error {
	return nil
}
