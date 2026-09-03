package cluster

import (
	_ "embed"
	"encoding/hex"
)

// WarmupInput is the stateless input of the 60M gas PUSH28 block of the EEST
// tests-zkevm-benchmark@v0.8.2 release, test_stack.py::test_push with
// opcode_PUSH28 and gas-value_60M. Proving it drives a cold prover through its
// one-time compile and cache costs before any measured proof.
//
// The block splits into about 230 segments, and a coordinator hands segments
// to workers in order, so every worker of a cluster up to that size receives
// a shard and pays its own first-dispatch costs here. An empty block gave one
// segment, warmed the first worker alone, and left the other fifteen of the
// rig to load their provers on the first measured block, which the
// coordinator then refused for over a minute.
//
//go:embed warmup_input.bin
var WarmupInput []byte

// WarmupOutput is the stateless output the warmup block commits to. Every
// guest that computes the block commits to the same output, so a warmup
// proof committing to anything else proves the guest did not run the block,
// and warmed nothing.
var WarmupOutput = mustHex("c26fa548f160616d5ad455673ec0775fba5c1453196f8c8825af28391fa53bcb0101000000000000000115")

func mustHex(text string) []byte {
	decoded, err := hex.DecodeString(text)
	if err != nil {
		panic(err)
	}
	return decoded
}
