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
//	ProgramVK    { vk: Vec<u64> }
//
// Only the public values are consumed. The proof itself is not verified,
// validity is decided by comparing the public values to the expected output.
const (
	// programVKWords and publicValuesBytes are the fixed lengths of the
	// envelope's trailing fields.
	programVKWords    = 4
	publicValuesBytes = 256
	// vadcopFinalHashFamily is the hash family every accepted proof carries.
	vadcopFinalHashFamily = "Poseidon1"
)

// decodeProofPublicValues extracts the committed public values from a proof
// envelope, rejecting envelopes of any other shape.
func decodeProofPublicValues(envelope []byte) ([]byte, error) {
	reader := &bincodeReader{buf: envelope}

	switch variant := reader.discriminant(); {
	case reader.err != nil:
	case variant == 0:
		reader.skipU64Vec()         // proof
		reader.skipU64Vec()         // zisk_vk
		minimal := reader.boolean() // minimal
		hashFamily := reader.utf8() // hash
		if reader.err == nil && (!minimal || hashFamily != vadcopFinalHashFamily) {
			return nil, fmt.Errorf("proof body is not a minimal %s vadcop proof", vadcopFinalHashFamily)
		}
	case variant == 1:
		return nil, fmt.Errorf("plonk proof body is not supported")
	default:
		return nil, fmt.Errorf("unknown proof body variant %d", variant)
	}

	publicValues := reader.byteVec()
	vkWords := reader.u64VecLen()
	if reader.err != nil {
		return nil, fmt.Errorf("malformed proof envelope: %w", reader.err)
	}
	if vkWords != programVKWords {
		return nil, fmt.Errorf("program vk holds %d words, expected %d", vkWords, programVKWords)
	}
	if len(publicValues) != publicValuesBytes {
		return nil, fmt.Errorf("public values hold %d bytes, expected %d", len(publicValues), publicValuesBytes)
	}
	return publicValues, nil
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
	switch prefix[0] {
	case 251:
		return uint64(binary.LittleEndian.Uint16(r.take(2)))
	case 252:
		return uint64(binary.LittleEndian.Uint32(r.take(4)))
	case 253:
		return binary.LittleEndian.Uint64(r.take(8))
	case 254, 255:
		r.fail("integer wider than 64 bits at offset %d", r.off-1)
		return 0
	default:
		return uint64(prefix[0])
	}
}

// discriminant decodes an enum variant index.
func (r *bincodeReader) discriminant() uint64 {
	return r.varint()
}

func (r *bincodeReader) length() int {
	length := r.varint()
	if r.err == nil && length > uint64(len(r.buf)-r.off) {
		// Every element occupies at least one byte, so a length beyond the
		// remaining input is malformed regardless of element width.
		r.fail("length %d exceeds remaining input", length)
	}
	return int(length)
}

// skipU64Vec reads over a Vec<u64> without keeping its elements.
func (r *bincodeReader) skipU64Vec() {
	for range r.length() {
		r.varint()
	}
}

// u64VecLen reads over a Vec<u64> and returns its element count.
func (r *bincodeReader) u64VecLen() int {
	length := r.length()
	for range length {
		r.varint()
	}
	return length
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
