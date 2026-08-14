package zisk

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readFixture loads one of the testdata fixtures, which come from two
// sources. proof.bin, program_vk.bin and public_values.bin are the zisk
// verifier fixtures ere v0.15.0 ships in crates/verifier/zisk/tests/fixtures,
// a VadcopFinalProof in bincode's legacy configuration with the 32 byte
// program vk it proves under and the 256 bytes it commits to. The
// cluster-prefixed three are a warmup block proved by a real coordinator, the
// envelope it returned, the verifying key ere-guests v0.15.0 publishes for
// that guest, and the public values verification yields.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	fixture, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

// appendVarint encodes a bincode standard-configuration unsigned integer.
func appendVarint(buf []byte, value uint64) []byte {
	switch {
	case value < 251:
		return append(buf, byte(value))
	case value <= math.MaxUint16:
		return binary.LittleEndian.AppendUint16(append(buf, 251), uint16(value))
	case value <= math.MaxUint32:
		return binary.LittleEndian.AppendUint32(append(buf, 252), uint32(value))
	default:
		return binary.LittleEndian.AppendUint64(append(buf, 253), value)
	}
}

func appendU64Vec(buf []byte, words []uint64) []byte {
	buf = appendVarint(buf, uint64(len(words)))
	for _, word := range words {
		buf = appendVarint(buf, word)
	}
	return buf
}

// envelope describes a proof envelope to encode, the writer mirror of the
// reader proof.go implements, defaulting through validEnvelope to the
// accepted shape.
type envelope struct {
	plonk        bool
	minimal      bool
	hashFamily   string
	proofWords   []uint64
	ziskVKWords  []uint64
	publicValues []byte
	vkWords      []uint64
}

func validEnvelope() envelope {
	return envelope{
		minimal:      true,
		hashFamily:   vadcopFinalHashFamily,
		proofWords:   []uint64{1, 2, 3},
		ziskVKWords:  []uint64{4, 300},
		publicValues: samplePublicValues(),
		vkWords:      []uint64{5, 6, 7, 8},
	}
}

func (e envelope) encode() []byte {
	var buf []byte
	if e.plonk {
		buf = appendVarint(buf, 1)
	} else {
		buf = appendVarint(buf, 0)
		buf = appendU64Vec(buf, e.proofWords)
		buf = appendU64Vec(buf, e.ziskVKWords)
		if e.minimal {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		buf = appendVarint(buf, uint64(len(e.hashFamily)))
		buf = append(buf, e.hashFamily...)
	}
	buf = appendVarint(buf, uint64(len(e.publicValues)))
	buf = append(buf, e.publicValues...)
	buf = appendU64Vec(buf, e.vkWords)
	// ProgramVK closes with the hash_mode discriminant, which decoding never
	// reaches.
	return appendVarint(buf, 0)
}

// samplePublicValues is a 33 byte commitment counting up from 1 followed by
// a zero tail.
func samplePublicValues() []byte {
	publicValues := make([]byte, 256)
	for i := range 33 {
		publicValues[i] = byte(i + 1)
	}
	return publicValues
}

// proof is the VadcopFinalProof ere's verifier accepts, decoded from bincode's
// legacy configuration, where every length and every word is eight
// little-endian bytes. An envelope is the cluster's own framing of the same
// proof, which is what a transcode turns back into this.
type proof struct {
	proofWords   []uint64
	publicValues []uint64
	compressed   bool
	hashFamily   string
}

func decodeProof(t *testing.T, encoded []byte) proof {
	t.Helper()
	offset := 0
	word := func() uint64 {
		if offset+8 > len(encoded) {
			t.Fatalf("proof ends at offset %d", offset)
		}
		value := binary.LittleEndian.Uint64(encoded[offset:])
		offset += 8
		return value
	}
	words := func() []uint64 {
		vec := make([]uint64, word())
		for i := range vec {
			vec[i] = word()
		}
		return vec
	}

	decoded := proof{proofWords: words(), publicValues: words()}
	decoded.compressed = encoded[offset] == 1
	offset++
	length := int(word())
	decoded.hashFamily = string(encoded[offset : offset+length])
	offset += length
	if offset != len(encoded) {
		t.Fatalf("proof leaves %d trailing bytes", len(encoded)-offset)
	}
	return decoded
}

// widenWords packs words into little-endian bytes.
func widenWords(words []uint64) []byte {
	buf := make([]byte, 0, 8*len(words))
	for _, word := range words {
		buf = binary.LittleEndian.AppendUint64(buf, word)
	}
	return buf
}

// narrowWords packs words holding u32 values into little-endian bytes, the
// form an envelope carries its commitment in.
func narrowWords(t *testing.T, words []uint64) []byte {
	t.Helper()
	buf := make([]byte, 0, 4*len(words))
	for _, word := range words {
		if word > math.MaxUint32 {
			t.Fatalf("public value %d does not fit a u32", word)
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(word))
	}
	return buf
}

// fixtureEnvelope shapes the fixture proof the way a coordinator sends it,
// splitting the proof's public values back into the program vk and the
// commitment the envelope carries separately.
func fixtureEnvelope(t *testing.T) envelope {
	t.Helper()
	decoded := decodeProof(t, readFixture(t, "proof.bin"))
	return envelope{
		minimal:      true,
		hashFamily:   vadcopFinalHashFamily,
		proofWords:   decoded.proofWords,
		ziskVKWords:  []uint64{9, 10, 11, 12},
		publicValues: narrowWords(t, decoded.publicValues[programVKWords:]),
		vkWords:      decoded.publicValues[:programVKWords],
	}
}

// TestFixtureLayout anchors the transcode against the fixtures ere verifies
// with. A proof's public values open with the program vk and continue with
// the committed bytes widened word by word.
func TestFixtureLayout(t *testing.T) {
	decoded := decodeProof(t, readFixture(t, "proof.bin"))
	if !decoded.compressed || decoded.hashFamily != vadcopFinalHashFamily {
		t.Fatalf("compressed = %v, hash family = %q", decoded.compressed, decoded.hashFamily)
	}
	if want := programVKWords + publicValuesBytes/4; len(decoded.publicValues) != want {
		t.Fatalf("public values hold %d words, want %d", len(decoded.publicValues), want)
	}
	if got, want := widenWords(decoded.publicValues[:programVKWords]), readFixture(t, "program_vk.bin"); !bytes.Equal(got, want) {
		t.Errorf("leading public values = %x, want the program vk %x", got, want)
	}
	if got, want := narrowWords(t, decoded.publicValues[programVKWords:]), readFixture(t, "public_values.bin"); !bytes.Equal(got, want) {
		t.Errorf("trailing public values = %x, want %x", got, want)
	}
}

// TestTranscodeProofMatchesFixture reproduces the fixture proof byte for byte
// from an envelope of the shape the cluster returns.
func TestTranscodeProofMatchesFixture(t *testing.T) {
	got, err := transcodeProof(fixtureEnvelope(t).encode())
	if err != nil {
		t.Fatal(err)
	}
	if want := readFixture(t, "proof.bin"); !bytes.Equal(got, want) {
		t.Errorf("transcoded %d bytes, want the %d byte fixture", len(got), len(want))
	}
}

func TestTranscodeProofRejects(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*envelope)
		wantErr string
	}{
		{"plonk", func(e *envelope) { e.plonk = true }, "plonk"},
		{"not_minimal", func(e *envelope) { e.minimal = false }, "vadcop"},
		{"wrong_hash", func(e *envelope) { e.hashFamily = "Poseidon2" }, "vadcop"},
		{"short_publics", func(e *envelope) { e.publicValues = e.publicValues[:255] }, "public values"},
		{"short_vk", func(e *envelope) { e.vkWords = e.vkWords[:3] }, "program vk"},
		{"long_vk", func(e *envelope) { e.vkWords = append(e.vkWords, 9) }, "program vk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEnvelope()
			tc.mutate(&e)
			_, err := transcodeProof(e.encode())
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

// TestTranscodeProofRejectsTruncation sweeps every truncation point rather
// than sampling, because a cut landing immediately after a multi-byte varint
// marker is the case that reads past the end. The trailing hash_mode byte is
// left out of the sweep, decoding stops before it and losing it truncates
// nothing the transcode reads.
func TestTranscodeProofRejectsTruncation(t *testing.T) {
	valid := validEnvelope().encode()
	for size := range len(valid) - 1 {
		if _, err := transcodeProof(valid[:size]); err == nil {
			t.Errorf("expected error for envelope truncated to %d bytes", size)
		}
	}
}

// TestTranscodeProofRejectsOversizedEnvelope pins the cap that bounds what a
// coordinator reply can expand into on its way to the verifier, well above the
// size a real proof reaches.
func TestTranscodeProofRejectsOversizedEnvelope(t *testing.T) {
	if fixture := len(readFixture(t, "proof.bin")); fixture >= maxEnvelopeBytes {
		t.Fatalf("the fixture proof holds %d bytes, at or beyond the %d byte cap", fixture, maxEnvelopeBytes)
	}
	_, err := transcodeProof(make([]byte, maxEnvelopeBytes+1))
	if err == nil || !strings.Contains(err.Error(), "beyond the") {
		t.Errorf("err = %v, want the envelope cap to reject it", err)
	}
}

// TestTranscodeProofRejectsOversizedLength pins that a declared element count
// beyond the remaining input fails immediately. Returning the count anyway
// makes the caller allocate and loop over it.
func TestTranscodeProofRejectsOversizedLength(t *testing.T) {
	envelope := []byte{0, 253, 0, 0, 0, 0, 0, 1, 0, 0}
	done := make(chan error, 1)
	go func() {
		_, err := transcodeProof(envelope)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "exceeds remaining input") {
			t.Errorf("err = %v, want mention of %q", err, "exceeds remaining input")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("decode did not return, the declared length is being looped over")
	}
}
