package event

// import (
// 	"encoding/binary"
// 	"math"
// 	"testing"

// 	"github.com/onflow/flow-go/model/flow"
// 	"github.com/stretchr/testify/require"
// )

// // Test_lookupKey_Bytes tests the lookup key encoding.
// func Test_lookupKey_Bytes(t *testing.T) {
// 	t.Parallel()

// 	expectedHeight := uint64(777)
// 	key := newPrimaryKey(expectedHeight, flow.EventType("A.1234567890abcdef.SomeToken.TokensWithdrawn"))

// 	bKey := key.Bytes()
// 	// Test encoded Owner and Key
// 	require.Equal(t, []byte("A.1234567890abcdef.SomeToken.TokensWithdrawn"), bKey[:len(bKey)-9])

// 	// Test encoded height
// 	actualHeight := binary.BigEndian.Uint64(bKey[len(bKey)-8:])
// 	require.Equal(t, math.MaxUint64-actualHeight, expectedHeight)

// 	// Test everything together
// 	require.Equal(t, []byte("A.1234567890abcdef.SomeToken.TokensWithdrawn/\xff\xff\xff\xff\xff\xff\xfc\xf6"), bKey)
// }
