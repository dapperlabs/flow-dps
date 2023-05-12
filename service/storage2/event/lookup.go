package event

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/onflow/flow-go/model/flow"
)

const (
	uint64Len = int(unsafe.Sizeof(uint64(0)))
	uint32Len = int(unsafe.Sizeof(uint32(0)))
)

// primaryKey is the encoded format of the storage key for looking up register value
type primaryKey struct {
	encodedKey []byte
	reference  pkeyReference
}

// pkeyReference is the fully qualified primary key reference that can be used to construct the primary key.
type pkeyReference struct {
	height uint64
	compactPkeyReference
}

// compactPkeyReference is used in contexts where the height is known.
// For example, when referencing primary key from secondary index, since secondary index key already contains height.
type compactPkeyReference struct {
	transactionIndex uint32
	eventIndex       uint32
}

func (r compactPkeyReference) Bytes() []byte {
	encoded := make([]byte, 0, uint32Len+uint32Len)

	return encoded
}

type compactPkeyReferences []compactPkeyReference

func newCompactReferencesFromBytes(buf []byte) (compactPkeyReferences, error) {
	const recordSize = uint32Len + uint32Len
	if len(buf)%recordSize != 0 {
		return nil, fmt.Errorf("invalid buffer length")
	}

	references := make(compactPkeyReferences, 0, len(buf)/uint32Len)
	for i := 0; i < len(buf); i += recordSize {
		reference := compactPkeyReference{
			transactionIndex: binary.BigEndian.Uint32(buf[i : i+uint32Len]),
			eventIndex:       binary.BigEndian.Uint32(buf[i+uint32Len : i+uint32Len+uint32Len]),
		}
		references = append(references, reference)
	}

	return references, nil
}

func (references compactPkeyReferences) Bytes() []byte {
	encoded := make([]byte, 0, uint32Len+uint32Len*len(references))
	for _, reference := range references {
		encoded = binary.BigEndian.AppendUint32(encoded, reference.transactionIndex)
		encoded = binary.BigEndian.AppendUint32(encoded, reference.eventIndex)
	}

	return encoded
}

func newPrimaryKey(reference pkeyReference) *primaryKey {
	keyLen := /* height */ uint64Len +
		/* TransactionIndex */ uint32Len +
		/* EventIndex */ uint32Len

	encoded := make([]byte, 0, keyLen)
	encoded = binary.BigEndian.AppendUint64(encoded, reference.height)
	encoded = binary.BigEndian.AppendUint32(encoded, reference.transactionIndex)
	encoded = binary.BigEndian.AppendUint32(encoded, reference.eventIndex)

	key := primaryKey{
		reference:  reference,
		encodedKey: encoded,
	}
	return &key
}

// Bytes returns the encoded lookup key.
func (p *primaryKey) Bytes() []byte {
	return p.encodedKey
}

type secondaryIndices struct {
	keys  []string
	value compactPkeyReference
}

// SecondaryIndices returns the encoded secondary keys.
func (p primaryKey) SecondaryIndices(eventType flow.EventType) (secondaryIndices, error) {
	parsedEvent, err := ParseEvent(string(eventType))
	if err != nil {
		return secondaryIndices{}, fmt.Errorf("failed to parse event: %s: %w", eventType, err)
	}

	keys := make([]string, 0, 3)
	keys = append(keys, string(newSecondaryKey(p.reference.height, string(parsedEvent.Type))))
	if !parsedEvent.IsFlowEvent() {
		keys = append(keys, string(newSecondaryKey(p.reference.height, parsedEvent.AddressPrefix())))
		keys = append(keys, string(newSecondaryKey(p.reference.height, parsedEvent.ContractPrefix())))
	}

	return secondaryIndices{
		keys:  keys,
		value: p.reference.compactPkeyReference,
	}, nil
}

func newSecondaryKey(
	height uint64,
	key string,
) []byte {
	binaryKey := make([]byte, 0, len(key)+uint64Len)
	binaryKey = append(binaryKey, []byte(key)...)
	binaryKey = binary.BigEndian.AppendUint64(binaryKey, height)

	return binaryKey
}
