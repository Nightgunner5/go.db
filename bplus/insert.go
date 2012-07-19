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

	panic("TODO")
}
