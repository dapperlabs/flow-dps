package event

import (
	"path"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/require"
)

// Test_EventStorage_RoundTrip tests the round trip of event storage.
func Test_EventStorage_RoundTrip(t *testing.T) {
	t.Parallel()

	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()

	dbpath := path.Join(t.TempDir(), "roundtrip.db")
	s, err := NewStorage(dbpath, cache)
	require.NoError(t, err)
	require.NotNil(t, s)

	eventType1 := "A.1234567890abcdef.SomeToken.TokensWithdrawn"
	expectedValue1 := []byte("value1")

	eventType2 := "A.1234567890abcdef.SomeToken.TokensDeposited"
	expectedValue2 := []byte("value2")
	events := []flow.Event{
		{
			Type:             flow.EventType(eventType1),
			TransactionIndex: 1,
			EventIndex:       11,
			Payload:          expectedValue1,
		},
		{
			Type:             flow.EventType(eventType2),
			TransactionIndex: 2,
			EventIndex:       22,
			Payload:          expectedValue2,
		},
	}

	minHeight := uint64(2)
	err = s.BatchSetEvents(minHeight, events)
	require.NoError(t, err)

	gotEvents, err := s.GetEventsBySecondaryIndex(minHeight, eventType1)
	require.NoError(t, err)
	require.Len(t, gotEvents, 1)
	gotEvent := gotEvents[0]
	require.EqualValues(t, 11, gotEvent.EventIndex)
	require.EqualValues(t, 1, gotEvent.TransactionIndex)

	gotEvents, err = s.GetEventsBySecondaryIndex(minHeight+1, eventType1)
	require.NoError(t, err)
	require.Len(t, gotEvents, 0)

	gotEvents, err = s.GetEventsBySecondaryIndex(minHeight-1, eventType1)
	require.NoError(t, err)
	require.Len(t, gotEvents, 0)

	err = s.Close()
	require.NoError(t, err)
}

// Test_EventStorage_StreamSecondary tests streaming of events by secondary key.
func Test_EventStorage_StreamSecondary(t *testing.T) {
	t.Parallel()

	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()

	dbpath := path.Join(t.TempDir(), "stream.db")
	s, err := NewStorage(dbpath, cache)
	require.NoError(t, err)
	require.NotNil(t, s)

	eventType1 := "A.1234567890abcdef.SomeToken.TokensWithdrawn"
	expectedValue1 := []byte("value1")
	expectedValue11 := []byte("value11")

	eventType2 := "A.1234567890abcdef.SomeToken.TokensDeposited"
	expectedValue2 := []byte("value2")
	events1 := []flow.Event{
		{
			Type:             flow.EventType(eventType1),
			TransactionIndex: 1,
			EventIndex:       1,
			Payload:          expectedValue1,
		},
		{
			Type:             flow.EventType(eventType1),
			TransactionIndex: 1,
			EventIndex:       11,
			Payload:          expectedValue11,
		},
		{
			Type:             flow.EventType(eventType2),
			TransactionIndex: 2,
			EventIndex:       22,
			Payload:          expectedValue2,
		},
		{
			Type:             flow.EventType("A.1234567890abcdef.SomeToken.TokensWithdrawnn"),
			TransactionIndex: 1111,
			EventIndex:       1111,
			Payload:          nil,
		},
		{
			Type:             flow.EventType("A.1234567890abcdef.SomeToken.TokensWithdraw"),
			TransactionIndex: 1112,
			EventIndex:       1112,
			Payload:          nil,
		},
	}

	minHeight := uint64(2)
	err = s.BatchSetEvents(minHeight, events1)
	require.NoError(t, err)

	events2 := []flow.Event{
		{
			Type:             flow.EventType(eventType1),
			TransactionIndex: 111,
			EventIndex:       1111,
			Payload:          expectedValue1,
		},
		{
			Type:             flow.EventType("A.1234567890abcdef.SomeToken.TokensWithdraw"),
			TransactionIndex: 1,
			EventIndex:       2,
			Payload:          nil,
		},
	}

	err = s.BatchSetEvents(minHeight+1, events2)
	require.NoError(t, err)

	iter, err := s.StreamEventsBySecondaryIndex(minHeight, eventType1)
	require.NoError(t, err)
	require.NotNil(t, iter)

	gotEvents1 := make([]flow.Event, 0)
	numEvents1, numBlocks1 := 0, 0
	for iter.First(); iter.Valid(); iter.Next() {
		values, err := iter.ValueAndErr()
		require.NoError(t, err)
		require.NotNil(t, values)
		for _, event := range values {
			require.NotNil(t, event)
			gotEvents1 = append(gotEvents1, event)
			numEvents1++
		}
		numBlocks1++
	}
	require.Equal(t, 2, numBlocks1)
	require.Equal(t, 3, numEvents1)

	require.Len(t, gotEvents1, 3)
	require.EqualValues(t, events1[0], gotEvents1[0])
	require.EqualValues(t, events1[1], gotEvents1[1])
	require.EqualValues(t, events2[0], gotEvents1[2])

	err = iter.Close()
	require.NoError(t, err)

	iter, err = s.StreamEventsBySecondaryIndex(minHeight+1, eventType1)
	require.NoError(t, err)
	require.NotNil(t, iter)

	gotEvents2 := make([]flow.Event, 0)
	numEvents2, numBlocks2 := 0, 0
	for iter.First(); iter.Valid(); iter.Next() {
		values, err := iter.ValueAndErr()
		require.NoError(t, err)
		require.NotNil(t, values)
		for _, event := range values {
			require.NotNil(t, event)
			gotEvents2 = append(gotEvents2, event)
			numEvents2++
		}
		numBlocks2++
	}
	require.Equal(t, 1, numBlocks2)
	require.Equal(t, 1, numEvents2)
	require.Len(t, gotEvents2, 1)

	require.EqualValues(t, events2[0], gotEvents2[0])

	err = iter.Close()
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)
}
