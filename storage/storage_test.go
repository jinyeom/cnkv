package storage

import "testing"

func TestGetBucketIdx(t *testing.T) {
	type testCase struct {
		numBuckets uint32
		key        string
	}
	testNumBuckets := []uint32{1, 2, 4, 8, 16, 32}
	testKeys := []string{
		"",
		"a",
		"ab",
		"abc",
		"abcd",
		"hello, world",
		"hello, world!",
		// From https://pkg.go.dev/hash/fnv
		"Fowler-Noll-Vo (or FNV) is a non-cryptographic hash function created by Glenn Fowler, Landon Curt Noll, and Kiem-Phong Vo.",
	}
	testCases := make([]testCase, 0)
	for _, numBuckets := range testNumBuckets {
		for _, key := range testKeys {
			testCases = append(testCases, testCase{
				numBuckets: numBuckets,
				key:        key,
			})
		}
	}

	for i, testCase := range testCases {
		s := NewStorage(testCase.numBuckets)
		idx1 := s.getBucketIdx(testCase.key)
		idx2 := s.getBucketIdx(testCase.key)
		if idx1 != idx2 {
			t.Errorf("%d: hash function is not deterministic: %d != %d", i, idx1, idx2)
		}
		if idx1 >= testCase.numBuckets {
			t.Errorf("%d: expected index less than %d, got %d", i, testCase.numBuckets, idx1)
		}
	}
}

func TestPutGet(t *testing.T) {
	testCases := []struct {
		// inputs
		numBuckets   uint32
		keys, values []string
		check        []string
		// target outputs
		expected []string
	}{
		{
			numBuckets: 1,
			keys:       []string{""},
			values:     []string{"hello, world!"},
			check:      []string{""},
			expected:   []string{"hello, world!"},
		},
		{
			numBuckets: 1,
			keys:       []string{"key1", "key2"},
			values:     []string{"value1", "value2"},
			check:      []string{"key1", "key2"},
			expected:   []string{"value1", "value2"},
		},
		{
			numBuckets: 8,
			keys:       []string{"a", "b", "c", "d"},
			values:     []string{"value1", "value2", "value3", "value4"},
			check:      []string{"a", "b", "c", "d"},
			expected:   []string{"value1", "value2", "value3", "value4"},
		},
		{
			numBuckets: 8,
			keys:       []string{"hello", "hello", "hello", "hello"},
			values:     []string{"world", "mom", "dad", "bro"},
			check:      []string{"hello"},
			expected:   []string{"bro"},
		},
	}
	for i, testCase := range testCases {
		s := NewStorage(testCase.numBuckets)
		for idx, key := range testCase.keys {
			value := testCase.values[idx]
			s.Put(key, value)
		}
		for idx, key := range testCase.check {
			expected := testCase.expected[idx]
			value, err := s.Get(key)
			if err != nil {
				t.Errorf("%d: unexpected error: %v", i, err)
			}
			if value != expected {
				t.Errorf("%d: expected %s, got %s", i, expected, value)
			}
		}
	}
}
