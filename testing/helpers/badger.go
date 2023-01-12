package helpers

import (
	"testing"

	"github.com/outcaste-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func InMemoryDB(t *testing.T) *badger.DB {
	t.Helper()

	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil

	db, err := badger.Open(opts)
	require.NoError(t, err)

	return db
}
