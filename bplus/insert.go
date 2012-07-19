package bplus

import "os"

func findOwnerNode(nodes *os.File, key BPlusKey) (BPlusPointer, *BPlusNode) {
	loc := RootPointer
	node := loc.ReadNode(nodes)
	if node == nil {
		node = new(BPlusNode)
		return loc, node
	}
	for !node.IsLeaf() {
		next := node.FindKey(key)
		nextNode := next.ReadNode(nodes)
		if next == InvalidPointer || nextNode == nil {
			return loc, node
		}
		loc = next
		node = nextNode
	}
	return loc, node
}

func Insert(nodes, values *os.File, key BPlusKey, value BPlusValue) error {
	loc, node := findOwnerNode(nodes, key)
	pointer := value.Save(values)

	for node.Full() {
		loc, node = loc.Split(nodes, key)
	}

	err := node.Add(key, pointer)
	if err != nil {
		return err
	}
	return loc.WriteNode(nodes, node)
}

func findNextKey(nodes *os.File) BPlusKey {
	loc := RootPointer
	node := loc.ReadNode(nodes)
	if node == nil {
		return 0
	}
	for {
		index, next := 0, BPlusKey(0)
		for i, key := range node.Keys {
			if key > next {
				index = i
				next = key
			} else {
				break
			}
		}

		if node.IsLeaf() {
			return next + 1
		}

		loc = node.Children[index]
		node = loc.ReadNode(nodes)
	}
	panic("unreachable")
}

func InsertAtEnd(nodes, values *os.File, value BPlusValue) BPlusKey {
	key := findNextKey(nodes)

	Insert(nodes, values, key, value)

	return key
}
