package payload

import (
	"path"
	"testing"

	"github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/require"
)

// Test_PayloadStorage_RoundTrip tests the round trip of a payload storage.
func Test_PayloadStorage_RoundTrip(t *testing.T) {
	t.Parallel()

	dbpath := path.Join(t.TempDir(), "roundtrip.db")
	s, err := NewPayloadStorage(dbpath, 1*1024*1024)
	require.NoError(t, err)
	require.NotNil(t, s)

	key1 := flow.RegisterID{Owner: "owner", Key: "key1"}
	expectedValue1 := []byte("value1")
	entries := flow.RegisterEntries{
		{Key: key1, Value: expectedValue1},
	}

	minHeight := uint64(2)
	err = s.SetBatch(minHeight, entries)
	require.NoError(t, err)

	value1, err := s.Get(minHeight, key1)
	require.NoError(t, err)
	require.Equal(t, expectedValue1, value1)

	value1, err = s.Get(minHeight+1, key1)
	require.NoError(t, err)
	require.Equal(t, expectedValue1, value1)

	value1, err = s.Get(minHeight-1, key1)
	require.Error(t, err)
	actualErr := &ErrNotFound{}
	require.ErrorAs(t, err, &actualErr)
	require.Equal(t, key1, actualErr.RegisterID)
	require.Equal(t, minHeight-1, actualErr.Height)
	require.Nil(t, value1)

	err = s.Close()
	require.NoError(t, err)
}

func Test_PayloadStorage_Versioning(t *testing.T) {
	t.Parallel()

	dbpath := path.Join(t.TempDir(), "versionning.db")
	s, err := NewPayloadStorage(dbpath, 1*1024*1024)
	require.NoError(t, err)
	require.NotNil(t, s)

	// Save key11 is a prefix of the key1 and we save it first.
	// It should be invisible for our prefix scan.
	key11 := flow.RegisterID{Owner: "owner", Key: "key11"}
	expectedValue11 := []byte("value11")

	key1 := flow.RegisterID{Owner: "owner", Key: "key1"}
	expectedValue1 := []byte("value1")
	entries1 := flow.RegisterEntries{
		{Key: key1, Value: expectedValue1},
		{Key: key11, Value: expectedValue11},
	}

	height1 := uint64(1)
	err = s.SetBatch(height1, entries1)
	require.NoError(t, err)

	// Test non-existent prefix.
	key := flow.RegisterID{Owner: "owner", Key: "key"}
	value0, err := s.Get(height1, key)
	require.Error(t, err)
	actualErr0 := &ErrNotFound{}
	require.ErrorAs(t, err, &actualErr0)
	require.Equal(t, key, actualErr0.RegisterID)
	require.Equal(t, height1, actualErr0.Height)
	require.Nil(t, value0)

	// Add new version of key1.
	height3 := uint64(3)
	expectedValue1ge3 := []byte("value1ge3")
	entries3 := flow.RegisterEntries{
		{Key: key1, Value: expectedValue1ge3},
	}
	err = s.SetBatch(height3, entries3)
	require.NoError(t, err)

	value1, err := s.Get(height1, key1)
	require.NoError(t, err)
	require.Equal(t, expectedValue1, value1)

	value1, err = s.Get(height3-1, key1)
	require.NoError(t, err)
	require.Equal(t, expectedValue1, value1)

	// test new version
	value1, err = s.Get(height3, key1)
	require.NoError(t, err)
	require.Equal(t, expectedValue1ge3, value1)

	value1, err = s.Get(height3+1, key1)
	require.NoError(t, err)
	require.Equal(t, expectedValue1ge3, value1)

	err = s.Close()
	require.NoError(t, err)
}
