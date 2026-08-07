package zisk

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// appendVarint encodes a bincode standard-configuration unsigned integer,
// covering the widths the vectors need.
func appendVarint(buf []byte, value uint64) []byte {
	if value < 251 {
		return append(buf, byte(value))
	}
	return binary.LittleEndian.AppendUint16(append(buf, 251), uint16(value))
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
	publicValues []byte
	vkWords      []uint64
}

func validEnvelope() envelope {
	return envelope{
		minimal:      true,
		hashFamily:   "Poseidon1",
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
		buf = appendU64Vec(buf, []uint64{1, 2, 3})
		buf = appendU64Vec(buf, []uint64{4, 300})
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
	return appendU64Vec(buf, e.vkWords)
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

func TestDecodeProofPublicValues(t *testing.T) {
	got, err := decodeProofPublicValues(validEnvelope().encode())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, samplePublicValues()) {
		t.Errorf("public values = %x", got)
	}
}

func TestDecodeProofPublicValuesRejects(t *testing.T) {
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEnvelope()
			tc.mutate(&e)
			_, err := decodeProofPublicValues(e.encode())
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestDecodeProofPublicValuesRejectsTruncation(t *testing.T) {
	valid := validEnvelope().encode()
	for _, size := range []int{0, 1, len(valid) / 2, len(valid) - 1} {
		if _, err := decodeProofPublicValues(valid[:size]); err == nil {
			t.Errorf("expected error for envelope truncated to %d bytes", size)
		}
	}
}
