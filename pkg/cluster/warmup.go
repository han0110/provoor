package cluster

import _ "embed"

// WarmupInput is the stateless input of mainnet block 25580396, an empty
// block. Proving it drives a cold prover through its one-time compile and
// cache costs before any measured proof.
//
//go:embed warmup_input.bin
var WarmupInput []byte
