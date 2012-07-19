package bplus

import (
	"encoding/binary"
	"errors"
	"os"
)

var (
	ErrInvalidPointer = errors.New("invalid pointer")
	ErrNodeFull       = errors.New("node is full")
	ErrDuplicateKey   = errors.New("duplicate key")
)

type BPlusVersion byte

const Version BPlusVersion = 0
const HeaderLength = 1 // BPlusVersion

type BPlusPointer uint64

const PointerSize = 8

func (pointer BPlusPointer) ReadNode(f *os.File) *BPlusNode {
	if pointer == InvalidPointer || pointer > IndexPointer {
		return nil
	}

	var buf [NodeSize]byte
	if _, err := f.ReadAt(buf[:], int64(pointer)); err != nil {
		return nil
	}

	node := new(BPlusNode)
	for i := 0; i < KeySize*Order; i += KeySize {
		node.Keys[i/KeySize] = UnserializeKey(buf[i : i+KeySize])
	}
	for i := KeySize * Order; i < KeySize*Order+PointerSize*Order; i += PointerSize {
		node.Children[(i-KeySize*Order)/PointerSize] = UnserializePointer(buf[i : i+PointerSize])
	}
	node.Next = UnserializePointer(buf[NodeSize-PointerSize:])
	return node
}

func (pointer BPlusPointer) WriteNode(f *os.File, node *BPlusNode) error {
	if pointer == InvalidPointer || pointer > IndexPointer {
		return ErrInvalidPointer
	}

	var buf = make([]byte, 0, NodeSize)

	for i := 0; i < Order; i++ {
		buf = append(buf, SerializeKey(node.Keys[i])...)
	}
	for i := 0; i < Order; i++ {
		buf = append(buf, SerializePointer(node.Children[i])...)
	}
	buf = append(buf, SerializePointer(node.Next)...)

	_, err := f.WriteAt(buf, int64(pointer))
	if err != nil {
		return err
	}

	return nil
}

func findParent(nodes *os.File, child BPlusPointer, key BPlusKey) (BPlusPointer, *BPlusNode) {
	loc := RootPointer
	node := loc.ReadNode(nodes)
	for {
		next := node.FindKey(key)
		if next == child {
			return loc, node
		}
		loc = next
		node = loc.ReadNode(nodes)
	}
	panic("unreachable")
}

func (pointer BPlusPointer) Split(nodes *os.File, next BPlusKey) (BPlusPointer, *BPlusNode) {
	toSplit := pointer.ReadNode(nodes)

	left, right := new(BPlusNode), new(BPlusNode)
	leftPos, rightPos := pointer, right.Allocate(nodes)

	left.Next = rightPos
	right.Next = toSplit.Next

	for i := 0; i < Order/2; i++ {
		left.Add(toSplit.Keys[i], toSplit.Children[i])
	}
	for i := Order / 2; i < Order; i++ {
		right.Add(toSplit.Keys[i], toSplit.Children[i])
	}

	if leftPos == RootPointer {
		leftPos = left.Allocate(nodes)
		root := new(BPlusNode)
		root.Add(left.Keys[0], leftPos)
		root.Add(right.Keys[0], rightPos)
		RootPointer.WriteNode(nodes, root)
	}

	leftPos.WriteNode(nodes, left)
	rightPos.WriteNode(nodes, right)

	if leftPos == pointer {
		parentPos, parent := findParent(nodes, pointer, next)
		for parent.Full() {
			parentPos, parent = parentPos.Split(nodes, right.Keys[0])
		}
		parent.Add(right.Keys[0], rightPos)
		parentPos.WriteNode(nodes, parent)
	}

	if right.Keys[0] <= next {
		return rightPos, right
	}
	return leftPos, left
}

func (pointer BPlusPointer) ReadValue(f *os.File) BPlusValue {
	if pointer == InvalidPointer || pointer <= IndexPointer {
		return nil
	}
	f.Seek(int64(pointer%ValueModulo), os.SEEK_SET)
	var b [8]byte
	f.Read(b[:])
	value := make(BPlusValue, binary.LittleEndian.Uint64(b[:]))
	f.Read(value)
	return value
}

// Pointers use little endian encoding. See below.
func SerializePointer(p BPlusPointer) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(p))
	return b[:]
}

func UnserializePointer(b []byte) BPlusPointer {
	return BPlusPointer(binary.LittleEndian.Uint64(b))
}

// Keys use big endian encoding. This should confuse people enough to make them ignore my crappy implementation of B+ trees.
func SerializeKey(k BPlusKey) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(k))
	return b[:]
}

func UnserializeKey(b []byte) BPlusKey {
	return BPlusKey(binary.BigEndian.Uint64(b))
}

const InvalidPointer BPlusPointer = 0                // A pointer to byte 0 would be the version number.
const RootPointer BPlusPointer = 1                   // The root node
const IndexPointer BPlusPointer = 0x7FFFFFFFFFFFFFFF // Any pointer greater than this is considered a pointer to a values file (modulo 0x8000000000000000). Any pointer less or equal is to an index file.
const ValueModulo BPlusPointer = 0x8000000000000000

const Order = 0x80 // This is a default, but currently unchangeable

type BPlusKey uint64 // TODO: Is there another type that might be better for use as keys?

const KeySize = 8

type BPlusNode struct {
	Keys     [Order]BPlusKey     // The number of keys should be at least Order/2 for any node other than the root node.
	Children [Order]BPlusPointer // Number of keys and number of children must be the same.
	Next     BPlusPointer        // InvalidPointer or the next BPlusNode in order. The root node uses this for an extra child space.
}

const NodeSize = Order*KeySize + Order*PointerSize + PointerSize

func (node *BPlusNode) Full() bool {
	return node.Children[Order-1] != InvalidPointer
}

func (node *BPlusNode) Add(key BPlusKey, pointer BPlusPointer) error {
	if node.Full() {
		return ErrNodeFull
	}

	for i, k := range node.Keys {
		if k == key {
			return ErrDuplicateKey
		}
		if node.Children[i] == InvalidPointer {
			node.Keys[i] = key
			node.Children[i] = pointer
			return nil
		}
		if k < key {
			copy(node.Keys[i+1:], node.Keys[i:])
			copy(node.Children[i+1:], node.Children[i:])
			node.Keys[i] = key
			node.Children[i] = pointer
			return nil
		}
	}
	return ErrNodeFull // Shouldn't be able to get this far, but whatevs
}

func (BPlusNode) Allocate(nodes *os.File) BPlusPointer {
	pointer, _ := nodes.Seek(0, os.SEEK_END)
	nodes.Write(make([]byte, NodeSize))
	return BPlusPointer(pointer)
}

func (node *BPlusNode) IsLeaf() bool {
	return node.Children[0] == InvalidPointer || node.Children[0] > IndexPointer
}

func (node *BPlusNode) FindKey(key BPlusKey) BPlusPointer {
	for i, k := range node.Keys {
		if k <= key {
			return node.Children[i]
		}
	}
	return node.Next
}

func (node *BPlusNode) GetValue(values *os.File, key BPlusKey) BPlusValue {
	if !node.IsLeaf() {
		return nil
	}

	for i, k := range node.Keys {
		if k == key {
			return node.Children[i].ReadValue(values)
		}
		if k < key {
			return nil
		}
	}
	return nil
}

type BPlusValue []byte

func (value BPlusValue) Save(values *os.File) BPlusPointer {
	pointer, err := values.Seek(0, os.SEEK_END)
	if err != nil {
		return InvalidPointer
	}

	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(len(value)))
	values.Write(b[:])
	values.Write(value)

	return BPlusPointer(pointer) + ValueModulo
}

func (value BPlusValue) Equal(other BPlusValue) bool {
	if len(value) != len(other) {
		return false
	}

	for i := 0; i < len(value); i++ {
		if value[i] != other[i] {
			return false
		}
	}

	return true
}
