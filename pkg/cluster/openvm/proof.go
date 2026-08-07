package openvm

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Guest commitment shape. Stateless-validator guests commit 256 bytes of
// public values, carried as 128 BabyBear field elements each holding one
// little-endian u16.
const (
	publicValuesBytes    = 256
	numUserPublicValues  = 128
	publicValuesTailSize = 4 + numUserPublicValues*4 + 32 + 1
)

// decodeProofPublicValues extracts the guest's public values from a proof
// envelope without decoding the enclosing STARK. The envelope's codec layout
// ends, for a proof without deferrals, with the user public values element
// count as a little-endian u32, the elements as canonical BabyBear u32s, a
// 32 byte commitment digest, and a zero deferral flag byte. The fixed
// element count anchors the tail parse, a count mismatch means the guest
// commits a different shape and the offsets would be wrong, so it fails
// loudly instead.
func decodeProofPublicValues(proof []byte) ([]byte, error) {
	if len(proof) < publicValuesTailSize {
		return nil, fmt.Errorf("proof of %d bytes is shorter than its %d byte public-values tail", len(proof), publicValuesTailSize)
	}
	if flag := proof[len(proof)-1]; flag != 0 {
		return nil, fmt.Errorf("deferral flag %d, expected a proof without deferrals", flag)
	}
	tail := proof[len(proof)-publicValuesTailSize:]
	if count := binary.LittleEndian.Uint32(tail); count != numUserPublicValues {
		return nil, fmt.Errorf("public values count %d, expected %d", count, numUserPublicValues)
	}

	publicValues := make([]byte, 0, publicValuesBytes)
	for i := range numUserPublicValues {
		element := binary.LittleEndian.Uint32(tail[4+i*4:])
		if element > math.MaxUint16 {
			return nil, fmt.Errorf("public value %d is %d, beyond the u16 range", i, element)
		}
		publicValues = append(publicValues, byte(element), byte(element>>8))
	}
	return publicValues, nil
}
