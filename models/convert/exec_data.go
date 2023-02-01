package convert

import (
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow/protobuf/go/flow/entities"
)

func MessageToTrieUpdate(m *entities.TrieUpdate) (*ledger.TrieUpdate, error) {
	rootHash, err := ledger.ToRootHash(m.GetRootHash())
	if err != nil {
		return nil, err
	}

	paths := make([]ledger.Path, len(m.GetPaths()))
	for i, path := range m.GetPaths() {
		convertedPath, err := ledger.ToPath(path)
		if err != nil {
			return nil, err
		}
		paths[i] = convertedPath
	}

	payloads := make([]*ledger.Payload, len(m.Payloads))
	for i, payload := range m.GetPayloads() {
		keyParts := make([]ledger.KeyPart, len(payload.GetKeyPart()))
		for j, keypart := range payload.GetKeyPart() {
			keyParts[j] = ledger.NewKeyPart(uint16(keypart.GetType()), keypart.GetValue())
		}
		payloads[i] = ledger.NewPayload(ledger.NewKey(keyParts), payload.GetValue())
	}

	return &ledger.TrieUpdate{
		RootHash: rootHash,
		Paths:    paths,
		Payloads: payloads,
	}, nil
}
