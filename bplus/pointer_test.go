package bplus

import (
	"testing"
	"testing/quick"
)

func TestKeySerialization(t *testing.T) {
	t.Parallel()
	if err := quick.Check(func(k uint64) bool {
		key := BPlusKey(k)
		serialized := SerializeKey(key)
		unserialized := UnserializeKey(serialized)
		// t.Logf("%x, %x", key, unserialized)
		return key == unserialized
	}, nil); err != nil {
		t.Error(err)
	}
}

func TestPointerSerialization(t *testing.T) {
	t.Parallel()
	if err := quick.Check(func(p uint64) bool {
		pointer := BPlusPointer(p)
		serialized := SerializePointer(pointer)
		unserialized := UnserializePointer(serialized)
		// t.Logf("%x, %x", pointer, unserialized)
		return pointer == unserialized
	}, nil); err != nil {
		t.Error(err)
	}
}
