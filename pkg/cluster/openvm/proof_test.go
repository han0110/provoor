package openvm

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// buildProof assembles a proof envelope whose tail carries the given
// elements, an opaque filler standing in for the encoded STARK until a
// verified proof captured from a cluster run replaces these inputs as
// fixtures. The filler exceeds the fixed tail length, so a mis-shaped tail
// reaches the count check rather than the length check.
func buildProof(elements []uint32, deferralFlag byte) []byte {
	proof := bytes.Repeat([]byte{0xEE}, 600)
	var tail bytes.Buffer
	_ = binary.Write(&tail, binary.LittleEndian, uint32(len(elements)))
	for _, element := range elements {
		_ = binary.Write(&tail, binary.LittleEndian, element)
	}
	tail.Write(bytes.Repeat([]byte{0xAA}, 32))
	tail.WriteByte(deferralFlag)
	return append(proof, tail.Bytes()...)
}

// sampleElements counts up scaled by three, staying within the u16 range
// the guest commitment uses.
func sampleElements() []uint32 {
	elements := make([]uint32, numUserPublicValues)
	for i := range elements {
		elements[i] = uint32(i * 3)
	}
	return elements
}

func samplePublicValues() []byte {
	publicValues := make([]byte, 0, publicValuesBytes)
	for _, element := range sampleElements() {
		publicValues = append(publicValues, byte(element), byte(element>>8))
	}
	return publicValues
}

func TestDecodeProofPublicValues(t *testing.T) {
	publicValues, err := decodeProofPublicValues(buildProof(sampleElements(), 0))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(publicValues, samplePublicValues()) {
		t.Errorf("publicValues = %x", publicValues)
	}
}

func TestDecodeProofPublicValuesRejects(t *testing.T) {
	nonU16 := sampleElements()
	nonU16[17] = 70000
	for _, tc := range []struct {
		name    string
		proof   []byte
		wantErr string
	}{
		{"deferral", buildProof(sampleElements(), 1), "deferral"},
		{"wrong count", buildProof(sampleElements()[:64], 0), "count"},
		{"non u16", buildProof(nonU16, 0), "u16"},
		{"short input", []byte{1, 2, 3}, "shorter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeProofPublicValues(tc.proof)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}
