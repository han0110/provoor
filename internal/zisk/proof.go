package zisk

import (
	"encoding/binary"
	"fmt"
)

// A completed prove job returns a proof envelope serialised with bincode's
// standard configuration, little endian with variable-width integers, laid
// out as
//
//	Proof      { body: ProofBody, program_vk: ProgramVK }
//	ProofBody  enum { Vadcop { proof: Vec<u64>, zisk_vk: Vec<u64>, kind: VadcopKind, hash: String, publics_full: Vec<u64> }, Plonk }
//	VadcopKind enum { Final, Recurser, Minimal }
//	ProgramVK  { vk: Vec<u64>, hash_mode: HashMode }
//
// The trailing hash_mode is never read. ere's verifier accepts the same proof
// under bincode's legacy configuration, little endian with fixed-width
// integers, so an envelope is transcoded before it is verified.
const (
	// programVKWords and publicValuesWords are the fixed lengths of the two
	// halves publics_full concatenates.
	programVKWords    = 4
	publicValuesWords = 64
	// vadcopKindMinimal is the VadcopKind a compressed proof carries, the
	// only flavour a prove job asks for.
	vadcopKindMinimal = 2
	// vadcopFinalHashFamily is the hash family every accepted proof carries.
	vadcopFinalHashFamily = "Poseidon1"
	// maxEnvelopeBytes caps the envelope a transcode reads. A vadcop final
	// proof runs under 300 KiB, and the cap keeps a coordinator's reply from
	// expanding eightfold into the heap.
	maxEnvelopeBytes = 1 << 20
)

// bincodeReader decodes bincode standard-configuration primitives with a
// sticky error, so a decode sequence checks the error once at the end.
type bincodeReader struct {
	buf []byte
	off int
	err error
}

// transcodeProof rewrites a proof envelope into the VadcopFinalProof ere's
// verifier accepts, proof, publics_full, compressed and hash in that order
// with every length and word in eight little-endian bytes. zisk_vk and the
// envelope's own program vk are dropped, since publics_full repeats the
// latter.
func transcodeProof(envelope []byte) ([]byte, error) {
	if len(envelope) > maxEnvelopeBytes {
		return nil, fmt.Errorf("proof envelope holds %d bytes, beyond the %d byte cap", len(envelope), maxEnvelopeBytes)
	}
	reader := &bincodeReader{buf: envelope}

	var proofWords, publicValues []uint64
	switch variant := reader.discriminant(); {
	case reader.err != nil:
	case variant == 0:
		proofWords = reader.u64Vec()   // proof
		reader.skipU64Vec()            // zisk_vk
		kind := reader.discriminant()  // kind
		hashFamily := reader.utf8()    // hash
		publicValues = reader.u64Vec() // publics_full
		if reader.err == nil && (kind != vadcopKindMinimal || hashFamily != vadcopFinalHashFamily) {
			return nil, fmt.Errorf("proof body is not a minimal %s vadcop proof", vadcopFinalHashFamily)
		}
	case variant == 1:
		return nil, fmt.Errorf("plonk proof body is not supported")
	default:
		return nil, fmt.Errorf("unknown proof body variant %d", variant)
	}

	vkWords := reader.u64Vec()
	if reader.err != nil {
		return nil, fmt.Errorf("malformed proof envelope: %w", reader.err)
	}
	if len(vkWords) != programVKWords {
		return nil, fmt.Errorf("program vk holds %d words, expected %d", len(vkWords), programVKWords)
	}
	if len(publicValues) != programVKWords+publicValuesWords {
		return nil, fmt.Errorf("public values hold %d words, expected %d", len(publicValues), programVKWords+publicValuesWords)
	}

	proof := make([]byte, 0, 8*(1+len(proofWords)+1+len(publicValues))+1+8+len(vadcopFinalHashFamily))
	proof = binary.LittleEndian.AppendUint64(proof, uint64(len(proofWords)))
	for _, word := range proofWords {
		proof = binary.LittleEndian.AppendUint64(proof, word)
	}
	proof = binary.LittleEndian.AppendUint64(proof, uint64(len(publicValues)))
	for _, word := range publicValues {
		proof = binary.LittleEndian.AppendUint64(proof, word)
	}
	proof = append(proof, 1) // compressed
	proof = binary.LittleEndian.AppendUint64(proof, uint64(len(vadcopFinalHashFamily)))
	return append(proof, vadcopFinalHashFamily...), nil
}

func (r *bincodeReader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

func (r *bincodeReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if r.off+n > len(r.buf) {
		r.fail("unexpected end of input at offset %d", r.off)
		return nil
	}
	taken := r.buf[r.off : r.off+n]
	r.off += n
	return taken
}

// varint decodes a variable-width unsigned integer. Values below 251 occupy
// one byte, and the markers 251, 252, and 253 prefix a little-endian u16,
// u32, and u64.
func (r *bincodeReader) varint() uint64 {
	prefix := r.take(1)
	if r.err != nil {
		return 0
	}
	var width int
	switch prefix[0] {
	case 251:
		width = 2
	case 252:
		width = 4
	case 253:
		width = 8
	case 254, 255:
		r.fail("integer wider than 64 bits at offset %d", r.off-1)
		return 0
	default:
		return uint64(prefix[0])
	}
	wide := r.take(width)
	if r.err != nil {
		return 0
	}
	var padded [8]byte
	copy(padded[:], wide)
	return binary.LittleEndian.Uint64(padded[:])
}

// discriminant decodes an enum variant index.
func (r *bincodeReader) discriminant() uint64 {
	return r.varint()
}

func (r *bincodeReader) length() int {
	length := r.varint()
	if r.err != nil {
		return 0
	}
	if length > uint64(len(r.buf)-r.off) {
		// Every element occupies at least one byte, so a length beyond the
		// remaining input is malformed regardless of element width.
		r.fail("length %d exceeds remaining input", length)
		return 0
	}
	return int(length)
}

// skipU64Vec reads over a Vec<u64> without keeping its elements.
func (r *bincodeReader) skipU64Vec() {
	for range r.length() {
		r.varint()
	}
}

// u64Vec reads a Vec<u64> and returns its elements.
func (r *bincodeReader) u64Vec() []uint64 {
	words := make([]uint64, r.length())
	for i := range words {
		words[i] = r.varint()
	}
	return words
}

func (r *bincodeReader) utf8() string {
	return string(r.take(r.length()))
}
