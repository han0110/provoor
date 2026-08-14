package zisk

import (
	"encoding/binary"
	"fmt"
)

// A completed prove job returns a proof envelope serialised with bincode's
// standard configuration, little endian with variable-width integers, laid
// out as
//
//	Proof        { body: ProofBody, publics: PublicValues, program_vk: ProgramVK }
//	ProofBody    enum { Vadcop { proof: Vec<u64>, zisk_vk: Vec<u64>, minimal: bool, hash: String }, Plonk }
//	PublicValues { data: Vec<u8> }
//	ProgramVK    { vk: Vec<u64>, hash_mode: HashMode }
//
// The trailing hash_mode is never read, decoding stops once the vk is in
// hand. ere's verifier accepts the same proof under bincode's legacy
// configuration instead, little endian with fixed-width integers, so an
// envelope is transcoded before it is verified.
const (
	// programVKWords and publicValuesBytes are the fixed lengths of the
	// envelope's trailing fields.
	programVKWords    = 4
	publicValuesBytes = 256
	// vadcopFinalHashFamily is the hash family every accepted proof carries.
	vadcopFinalHashFamily = "Poseidon1"
	// maxEnvelopeBytes caps the envelope a transcode will read. A vadcop
	// final proof runs under 300 KiB, so the margin is generous, and the cap
	// keeps a coordinator's reply from expanding eightfold into the heap on
	// its way to the verifier.
	maxEnvelopeBytes = 1 << 20
)

// transcodeProof rewrites a proof envelope into the VadcopFinalProof ere's
// verifier accepts, rejecting envelopes of any other shape. The result uses
// bincode's legacy configuration, so every length and every word occupies
// eight little-endian bytes, holding proof, public_values, compressed and
// hash in that order.
//
// The verified public values are the vk words followed by the envelope's
// commitment read as little-endian u32s, each widened to a word. zisk_vk is
// discarded, it does not identify the guest program.
func transcodeProof(envelope []byte) ([]byte, error) {
	if len(envelope) > maxEnvelopeBytes {
		return nil, fmt.Errorf("proof envelope holds %d bytes, beyond the %d byte cap", len(envelope), maxEnvelopeBytes)
	}
	reader := &bincodeReader{buf: envelope}

	var proofWords []uint64
	switch variant := reader.discriminant(); {
	case reader.err != nil:
	case variant == 0:
		proofWords = reader.u64Vec() // proof
		reader.skipU64Vec()          // zisk_vk
		minimal := reader.boolean()  // minimal
		hashFamily := reader.utf8()  // hash
		if reader.err == nil && (!minimal || hashFamily != vadcopFinalHashFamily) {
			return nil, fmt.Errorf("proof body is not a minimal %s vadcop proof", vadcopFinalHashFamily)
		}
	case variant == 1:
		return nil, fmt.Errorf("plonk proof body is not supported")
	default:
		return nil, fmt.Errorf("unknown proof body variant %d", variant)
	}

	publicValues := reader.byteVec()
	vkWords := reader.u64Vec()
	if reader.err != nil {
		return nil, fmt.Errorf("malformed proof envelope: %w", reader.err)
	}
	if len(vkWords) != programVKWords {
		return nil, fmt.Errorf("program vk holds %d words, expected %d", len(vkWords), programVKWords)
	}
	if len(publicValues) != publicValuesBytes {
		return nil, fmt.Errorf("public values hold %d bytes, expected %d", len(publicValues), publicValuesBytes)
	}

	proof := make([]byte, 0, 8*(1+len(proofWords)+1+programVKWords+publicValuesBytes/4)+1+8+len(vadcopFinalHashFamily))
	proof = binary.LittleEndian.AppendUint64(proof, uint64(len(proofWords)))
	for _, word := range proofWords {
		proof = binary.LittleEndian.AppendUint64(proof, word)
	}
	proof = binary.LittleEndian.AppendUint64(proof, uint64(programVKWords+publicValuesBytes/4))
	for _, word := range vkWords {
		proof = binary.LittleEndian.AppendUint64(proof, word)
	}
	for offset := 0; offset < publicValuesBytes; offset += 4 {
		proof = binary.LittleEndian.AppendUint64(proof, uint64(binary.LittleEndian.Uint32(publicValues[offset:])))
	}
	proof = append(proof, 1) // compressed
	proof = binary.LittleEndian.AppendUint64(proof, uint64(len(vadcopFinalHashFamily)))
	return append(proof, vadcopFinalHashFamily...), nil
}

// bincodeReader decodes bincode standard-configuration primitives with a
// sticky error, so a decode sequence checks the error once at the end.
type bincodeReader struct {
	buf []byte
	off int
	err error
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

func (r *bincodeReader) byteVec() []byte {
	return r.take(r.length())
}

func (r *bincodeReader) boolean() bool {
	value := r.take(1)
	if r.err != nil {
		return false
	}
	if value[0] > 1 {
		r.fail("invalid boolean %d at offset %d", value[0], r.off-1)
		return false
	}
	return value[0] == 1
}

func (r *bincodeReader) utf8() string {
	return string(r.take(r.length()))
}
