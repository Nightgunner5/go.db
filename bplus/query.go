package bplus

import "os"

// Get a value from the B+ tree in O(log n) time.
// Returns nil if the key does not exist in the node file.
func Search(nodes, values *os.File, key BPlusKey) BPlusValue {
	loc := RootPointer
	node := loc.ReadNode(nodes)
	if node == nil {
		return nil
	}
	for !node.IsLeaf() {
		next := node.FindKey(key)
		nextNode := next.ReadNode(nodes)
		if next == InvalidPointer || nextNode == nil {
			return nil
		}
		loc = next
		node = nextNode
	}
	return node.GetValue(values, key)
}

type Iterator interface {
	Valid() bool
	Next()
	Value() BPlusValue
	Key() BPlusKey
}

type iterator struct {
	nodes, values *os.File
	node          *BPlusNode
	index         uint8
}

var _ Iterator = new(iterator)

func (it *iterator) first() {
	node := RootPointer.ReadNode(it.nodes)
	for !node.IsLeaf() {
		node = node.Children[0].ReadNode(it.nodes)
	}
	it.node = node
}

func (it *iterator) Valid() bool {
	return it.node != nil && it.node.Children[it.index] > IndexPointer
}

func (it *iterator) Next() {
	if it.node == nil {
		return
	}
	it.index++
	if it.index >= Order || it.node.Children[it.index] == InvalidPointer {
		it.index = 0
		if it.node.Next == InvalidPointer {
			it.node = nil
		} else {
			it.node = it.node.Next.ReadNode(it.nodes)
		}
	}
}

func (it *iterator) Value() BPlusValue {
	return it.node.Children[it.index].ReadValue(it.values)
}

func (it *iterator) Key() BPlusKey {
	return it.node.Keys[it.index]
}

func GetAll(nodes, values *os.File) Iterator {
	it := new(iterator)
	it.nodes, it.values = nodes, values
	it.first()

	return it
}
