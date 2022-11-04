package immutable

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"golang.org/x/exp/constraints"
)

// Sorted map child node limit size.
const (
	sortedMapNodeSize = 32
)

// SortedMap represents a map of key/value pairs sorted by key. The sort order
// is determined by the Comparer used by the map.
//
// This map is implemented as a B+tree.
type SortedMap[K constraints.Ordered, V any] struct {
	size     int                 // total number of key/value pairs
	root     sortedMapNode[K, V] // root of b+tree
	comparer Comparer[K]
}

// NewSortedMap returns a new instance of SortedMap. If comparer is nil then
// a default comparer is set after the first key is inserted. Default comparers
// exist for int, string, and byte slice keys.
func NewSortedMap[K constraints.Ordered, V any](comparer Comparer[K]) *SortedMap[K, V] {
	return &SortedMap[K, V]{
		comparer: comparer,
	}
}

// Len returns the number of elements in the sorted map.
func (m *SortedMap[K, V]) Len() int {
	return m.size
}

// Get returns the value for a given key and a flag indicating if the key is set.
// The flag can be used to distinguish between a nil-set key versus an unset key.
func (m *SortedMap[K, V]) Get(key K) (V, bool) {
	if m.root == nil {
		var v V
		return v, false
	}
	return m.root.get(key, m.comparer)
}

// Set returns a copy of the map with the key set to the given value.
func (m *SortedMap[K, V]) Set(key K, value V) *SortedMap[K, V] {
	return m.set(key, value, false)
}

func (m *SortedMap[K, V]) set(key K, value V, mutable bool) *SortedMap[K, V] {
	// Set a comparer on the first value if one does not already exist.
	comparer := m.comparer
	if comparer == nil {
		comparer = NewComparer(key)
	}

	// Create copy, if necessary.
	other := m
	if !mutable {
		other = m.clone()
	}
	other.comparer = comparer

	// If no values are set then initialize with a leaf node.
	if m.root == nil {
		other.size = 1
		other.root = &sortedMapLeafNode[K, V]{entries: []mapEntry[K, V]{{key: key, value: value}}}
		return other
	}

	// Otherwise delegate to root node.
	// If a split occurs then grow the tree from the root.
	var resized bool
	newRoot, splitNode := m.root.set(key, value, comparer, mutable, &resized)
	if splitNode != nil {
		newRoot = newSortedMapBranchNode(newRoot, splitNode)
	}

	// Update root and size (if resized).
	other.size = m.size
	other.root = newRoot
	if resized {
		other.size++
	}
	return other
}

// Delete returns a copy of the map with the key removed.
// Returns the original map if key does not exist.
func (m *SortedMap[K, V]) Delete(key K) *SortedMap[K, V] {
	return m.delete(key, false)
}

func (m *SortedMap[K, V]) delete(key K, mutable bool) *SortedMap[K, V] {
	// Return original map if no keys exist.
	if m.root == nil {
		return m
	}

	// If the delete did not change the node then return the original map.
	var resized bool
	newRoot := m.root.delete(key, m.comparer, mutable, &resized)
	if !resized {
		return m
	}

	// Create copy, if necessary.
	other := m
	if !mutable {
		other = m.clone()
	}

	// Update root and size.
	other.size = m.size - 1
	other.root = newRoot
	return other
}

// clone returns a shallow copy of m.
func (m *SortedMap[K, V]) clone() *SortedMap[K, V] {
	other := *m
	return &other
}

// Iterator returns a new iterator for this map positioned at the first key.
func (m *SortedMap[K, V]) Iterator() *SortedMapIterator[K, V] {
	itr := &SortedMapIterator[K, V]{m: m}
	itr.First()
	return itr
}

// SortedMapBuilder represents an efficient builder for creating sorted maps.
type SortedMapBuilder[K constraints.Ordered, V any] struct {
	m *SortedMap[K, V] // current state
}

// NewSortedMapBuilder returns a new instance of SortedMapBuilder.
func NewSortedMapBuilder[K constraints.Ordered, V any](comparer Comparer[K]) *SortedMapBuilder[K, V] {
	return &SortedMapBuilder[K, V]{m: NewSortedMap[K, V](comparer)}
}

// SortedMap returns the current copy of the map.
// The returned map is safe to use even if after the builder continues to be used.
func (b *SortedMapBuilder[K, V]) Map() *SortedMap[K, V] {
	assert(b.m != nil, "immutable.SortedMapBuilder.Map(): duplicate call to fetch map")
	m := b.m
	b.m = nil
	return m
}

// Len returns the number of elements in the underlying map.
func (b *SortedMapBuilder[K, V]) Len() int {
	assert(b.m != nil, "immutable.SortedMapBuilder: builder invalid after Map() invocation")
	return b.m.Len()
}

// Get returns the value for the given key.
func (b *SortedMapBuilder[K, V]) Get(key K) (value V, ok bool) {
	assert(b.m != nil, "immutable.SortedMapBuilder: builder invalid after Map() invocation")
	return b.m.Get(key)
}

// Set sets the value of the given key. See SortedMap.Set() for additional details.
func (b *SortedMapBuilder[K, V]) Set(key K, value V) {
	assert(b.m != nil, "immutable.SortedMapBuilder: builder invalid after Map() invocation")
	b.m = b.m.set(key, value, true)
}

// Delete removes the given key. See SortedMap.Delete() for additional details.
func (b *SortedMapBuilder[K, V]) Delete(key K) {
	assert(b.m != nil, "immutable.SortedMapBuilder: builder invalid after Map() invocation")
	b.m = b.m.delete(key, true)
}

// Iterator returns a new iterator for the underlying map positioned at the first key.
func (b *SortedMapBuilder[K, V]) Iterator() *SortedMapIterator[K, V] {
	assert(b.m != nil, "immutable.SortedMapBuilder: builder invalid after Map() invocation")
	return b.m.Iterator()
}

// sortedMapNode represents a branch or leaf node in the sorted map.
type sortedMapNode[K constraints.Ordered, V any] interface {
	minKey() K
	indexOf(key K, c Comparer[K]) int
	get(key K, c Comparer[K]) (value V, ok bool)
	set(key K, value V, c Comparer[K], mutable bool, resized *bool) (sortedMapNode[K, V], sortedMapNode[K, V])
	delete(key K, c Comparer[K], mutable bool, resized *bool) sortedMapNode[K, V]
}

var _ sortedMapNode[string, any] = (*sortedMapBranchNode[string, any])(nil)
var _ sortedMapNode[string, any] = (*sortedMapLeafNode[string, any])(nil)

// sortedMapBranchNode represents a branch in the sorted map.
type sortedMapBranchNode[K constraints.Ordered, V any] struct {
	elems []sortedMapBranchElem[K, V]
}

// newSortedMapBranchNode returns a new branch node with the given child nodes.
func newSortedMapBranchNode[K constraints.Ordered, V any](children ...sortedMapNode[K, V]) *sortedMapBranchNode[K, V] {
	// Fetch min keys for every child.
	elems := make([]sortedMapBranchElem[K, V], len(children))
	for i, child := range children {
		elems[i] = sortedMapBranchElem[K, V]{
			key:  child.minKey(),
			node: child,
		}
	}

	return &sortedMapBranchNode[K, V]{elems: elems}
}

// minKey returns the lowest key stored in this node's tree.
func (n *sortedMapBranchNode[K, V]) minKey() K {
	return n.elems[0].node.minKey()
}

// indexOf returns the index of the key within the child nodes.
func (n *sortedMapBranchNode[K, V]) indexOf(key K, c Comparer[K]) int {
	if idx := sort.Search(len(n.elems), func(i int) bool { return c.Compare(n.elems[i].key, key) == 1 }); idx > 0 {
		return idx - 1
	}
	return 0
}

// get returns the value for the given key.
func (n *sortedMapBranchNode[K, V]) get(key K, c Comparer[K]) (value V, ok bool) {
	idx := n.indexOf(key, c)
	return n.elems[idx].node.get(key, c)
}

// set returns a copy of the node with the key set to the given value.
func (n *sortedMapBranchNode[K, V]) set(key K, value V, c Comparer[K], mutable bool, resized *bool) (sortedMapNode[K, V], sortedMapNode[K, V]) {
	idx := n.indexOf(key, c)

	// Delegate insert to child node.
	newNode, splitNode := n.elems[idx].node.set(key, value, c, mutable, resized)

	// Update in-place, if mutable.
	if mutable {
		n.elems[idx] = sortedMapBranchElem[K, V]{key: newNode.minKey(), node: newNode}
		if splitNode != nil {
			n.elems = append(n.elems, sortedMapBranchElem[K, V]{})
			copy(n.elems[idx+1:], n.elems[idx:])
			n.elems[idx+1] = sortedMapBranchElem[K, V]{key: splitNode.minKey(), node: splitNode}
		}

		// If the child splits and we have no more room then we split too.
		if len(n.elems) > sortedMapNodeSize {
			splitIdx := len(n.elems) / 2
			newNode := &sortedMapBranchNode[K, V]{elems: n.elems[:splitIdx:splitIdx]}
			splitNode := &sortedMapBranchNode[K, V]{elems: n.elems[splitIdx:]}
			return newNode, splitNode
		}
		return n, nil
	}

	// If no split occurs, copy branch and update keys.
	// If the child splits, insert new key/child into copy of branch.
	var other sortedMapBranchNode[K, V]
	if splitNode == nil {
		other.elems = make([]sortedMapBranchElem[K, V], len(n.elems))
		copy(other.elems, n.elems)
		other.elems[idx] = sortedMapBranchElem[K, V]{
			key:  newNode.minKey(),
			node: newNode,
		}
	} else {
		other.elems = make([]sortedMapBranchElem[K, V], len(n.elems)+1)
		copy(other.elems[:idx], n.elems[:idx])
		copy(other.elems[idx+1:], n.elems[idx:])
		other.elems[idx] = sortedMapBranchElem[K, V]{
			key:  newNode.minKey(),
			node: newNode,
		}
		other.elems[idx+1] = sortedMapBranchElem[K, V]{
			key:  splitNode.minKey(),
			node: splitNode,
		}
	}

	// If the child splits and we have no more room then we split too.
	if len(other.elems) > sortedMapNodeSize {
		splitIdx := len(other.elems) / 2
		newNode := &sortedMapBranchNode[K, V]{elems: other.elems[:splitIdx:splitIdx]}
		splitNode := &sortedMapBranchNode[K, V]{elems: other.elems[splitIdx:]}
		return newNode, splitNode
	}

	// Otherwise return the new branch node with the updated entry.
	return &other, nil
}

// delete returns a node with the key removed. Returns the same node if the key
// does not exist. Returns nil if all child nodes are removed.
func (n *sortedMapBranchNode[K, V]) delete(key K, c Comparer[K], mutable bool, resized *bool) sortedMapNode[K, V] {
	idx := n.indexOf(key, c)

	// Return original node if child has not changed.
	newNode := n.elems[idx].node.delete(key, c, mutable, resized)
	if !*resized {
		return n
	}

	// Remove child if it is now nil.
	if newNode == nil {
		// If this node will become empty then simply return nil.
		if len(n.elems) == 1 {
			return nil
		}

		// If mutable, update in-place.
		if mutable {
			copy(n.elems[idx:], n.elems[idx+1:])
			n.elems[len(n.elems)-1] = sortedMapBranchElem[K, V]{}
			n.elems = n.elems[:len(n.elems)-1]
			return n
		}

		// Return a copy without the given node.
		other := &sortedMapBranchNode[K, V]{elems: make([]sortedMapBranchElem[K, V], len(n.elems)-1)}
		copy(other.elems[:idx], n.elems[:idx])
		copy(other.elems[idx:], n.elems[idx+1:])
		return other
	}

	// If mutable, update in-place.
	if mutable {
		n.elems[idx] = sortedMapBranchElem[K, V]{key: newNode.minKey(), node: newNode}
		return n
	}

	// Return a copy with the updated node.
	other := &sortedMapBranchNode[K, V]{elems: make([]sortedMapBranchElem[K, V], len(n.elems))}
	copy(other.elems, n.elems)
	other.elems[idx] = sortedMapBranchElem[K, V]{
		key:  newNode.minKey(),
		node: newNode,
	}
	return other
}

type sortedMapBranchElem[K constraints.Ordered, V any] struct {
	key  K
	node sortedMapNode[K, V]
}

// sortedMapLeafNode represents a leaf node in the sorted map.
type sortedMapLeafNode[K constraints.Ordered, V any] struct {
	entries []mapEntry[K, V]
}

// minKey returns the first key stored in this node.
func (n *sortedMapLeafNode[K, V]) minKey() K {
	return n.entries[0].key
}

// indexOf returns the index of the given key.
func (n *sortedMapLeafNode[K, V]) indexOf(key K, c Comparer[K]) int {
	return sort.Search(len(n.entries), func(i int) bool {
		return c.Compare(n.entries[i].key, key) != -1 // GTE
	})
}

// get returns the value of the given key.
func (n *sortedMapLeafNode[K, V]) get(key K, c Comparer[K]) (value V, ok bool) {
	idx := n.indexOf(key, c)

	// If the index is beyond the entry count or the key is not equal then return 'not found'.
	if idx == len(n.entries) || c.Compare(n.entries[idx].key, key) != 0 {
		return value, false
	}

	// If the key matches then return its value.
	return n.entries[idx].value, true
}

// set returns a copy of node with the key set to the given value. If the update
// causes the node to grow beyond the maximum size then it is split in two.
func (n *sortedMapLeafNode[K, V]) set(key K, value V, c Comparer[K], mutable bool, resized *bool) (sortedMapNode[K, V], sortedMapNode[K, V]) {
	// Find the insertion index for the key.
	idx := n.indexOf(key, c)
	exists := idx < len(n.entries) && c.Compare(n.entries[idx].key, key) == 0

	// Update in-place, if mutable.
	if mutable {
		if !exists {
			*resized = true
			n.entries = append(n.entries, mapEntry[K, V]{})
			copy(n.entries[idx+1:], n.entries[idx:])
		}
		n.entries[idx] = mapEntry[K, V]{key: key, value: value}

		// If the key doesn't exist and we exceed our max allowed values then split.
		if len(n.entries) > sortedMapNodeSize {
			splitIdx := len(n.entries) / 2
			newNode := &sortedMapLeafNode[K, V]{entries: n.entries[:splitIdx:splitIdx]}
			splitNode := &sortedMapLeafNode[K, V]{entries: n.entries[splitIdx:]}
			return newNode, splitNode
		}
		return n, nil
	}

	// If the key matches then simply return a copy with the entry overridden.
	// If there is no match then insert new entry and mark as resized.
	var newEntries []mapEntry[K, V]
	if exists {
		newEntries = make([]mapEntry[K, V], len(n.entries))
		copy(newEntries, n.entries)
		newEntries[idx] = mapEntry[K, V]{key: key, value: value}
	} else {
		*resized = true
		newEntries = make([]mapEntry[K, V], len(n.entries)+1)
		copy(newEntries[:idx], n.entries[:idx])
		newEntries[idx] = mapEntry[K, V]{key: key, value: value}
		copy(newEntries[idx+1:], n.entries[idx:])
	}

	// If the key doesn't exist and we exceed our max allowed values then split.
	if len(newEntries) > sortedMapNodeSize {
		splitIdx := len(newEntries) / 2
		newNode := &sortedMapLeafNode[K, V]{entries: newEntries[:splitIdx:splitIdx]}
		splitNode := &sortedMapLeafNode[K, V]{entries: newEntries[splitIdx:]}
		return newNode, splitNode
	}

	// Otherwise return the new leaf node with the updated entry.
	return &sortedMapLeafNode[K, V]{entries: newEntries}, nil
}

// delete returns a copy of node with key removed. Returns the original node if
// the key does not exist. Returns nil if the removed key is the last remaining key.
func (n *sortedMapLeafNode[K, V]) delete(key K, c Comparer[K], mutable bool, resized *bool) sortedMapNode[K, V] {
	idx := n.indexOf(key, c)

	// Return original node if key is not found.
	if idx >= len(n.entries) || c.Compare(n.entries[idx].key, key) != 0 {
		return n
	}
	*resized = true

	// If this is the last entry then return nil.
	if len(n.entries) == 1 {
		return nil
	}

	// Update in-place, if mutable.
	if mutable {
		copy(n.entries[idx:], n.entries[idx+1:])
		n.entries[len(n.entries)-1] = mapEntry[K, V]{}
		n.entries = n.entries[:len(n.entries)-1]
		return n
	}

	// Return copy of node with entry removed.
	other := &sortedMapLeafNode[K, V]{entries: make([]mapEntry[K, V], len(n.entries)-1)}
	copy(other.entries[:idx], n.entries[:idx])
	copy(other.entries[idx:], n.entries[idx+1:])
	return other
}

// SortedMapIterator represents an iterator over a sorted map.
// Iteration can occur in natural or reverse order based on use of Next() or Prev().
type SortedMapIterator[K constraints.Ordered, V any] struct {
	m *SortedMap[K, V] // source map

	stack [32]sortedMapIteratorElem[K, V] // search stack
	depth int                             // stack depth
}

// Done returns true if no more key/value pairs remain in the iterator.
func (itr *SortedMapIterator[K, V]) Done() bool {
	return itr.depth == -1
}

// First moves the iterator to the first key/value pair.
func (itr *SortedMapIterator[K, V]) First() {
	if itr.m.root == nil {
		itr.depth = -1
		return
	}
	itr.stack[0] = sortedMapIteratorElem[K, V]{node: itr.m.root}
	itr.depth = 0
	itr.first()
}

// Last moves the iterator to the last key/value pair.
func (itr *SortedMapIterator[K, V]) Last() {
	if itr.m.root == nil {
		itr.depth = -1
		return
	}
	itr.stack[0] = sortedMapIteratorElem[K, V]{node: itr.m.root}
	itr.depth = 0
	itr.last()
}

// Seek moves the iterator position to the given key in the map.
// If the key does not exist then the next key is used. If no more keys exist
// then the iteartor is marked as done.
func (itr *SortedMapIterator[K, V]) Seek(key K) {
	if itr.m.root == nil {
		itr.depth = -1
		return
	}
	itr.stack[0] = sortedMapIteratorElem[K, V]{node: itr.m.root}
	itr.depth = 0
	itr.seek(key)
}

// Next returns the current key/value pair and moves the iterator forward.
// Returns a nil key if the there are no more elements to return.
func (itr *SortedMapIterator[K, V]) Next() (key K, value V, ok bool) {
	// Return nil key if iteration is complete.
	if itr.Done() {
		return key, value, false
	}

	// Retrieve current key/value pair.
	leafElem := &itr.stack[itr.depth]
	leafNode := leafElem.node.(*sortedMapLeafNode[K, V])
	leafEntry := &leafNode.entries[leafElem.index]
	key, value = leafEntry.key, leafEntry.value

	// Move to the next available key/value pair.
	itr.next()

	// Only occurs when iterator is done.
	return key, value, true
}

// next moves to the next key. If no keys are after then depth is set to -1.
func (itr *SortedMapIterator[K, V]) next() {
	for ; itr.depth >= 0; itr.depth-- {
		elem := &itr.stack[itr.depth]

		switch node := elem.node.(type) {
		case *sortedMapLeafNode[K, V]:
			if elem.index < len(node.entries)-1 {
				elem.index++
				return
			}
		case *sortedMapBranchNode[K, V]:
			if elem.index < len(node.elems)-1 {
				elem.index++
				itr.stack[itr.depth+1].node = node.elems[elem.index].node
				itr.depth++
				itr.first()
				return
			}
		}
	}
}

// Prev returns the current key/value pair and moves the iterator backward.
// Returns a nil key if the there are no more elements to return.
func (itr *SortedMapIterator[K, V]) Prev() (key K, value V, ok bool) {
	// Return nil key if iteration is complete.
	if itr.Done() {
		return key, value, false
	}

	// Retrieve current key/value pair.
	leafElem := &itr.stack[itr.depth]
	leafNode := leafElem.node.(*sortedMapLeafNode[K, V])
	leafEntry := &leafNode.entries[leafElem.index]
	key, value = leafEntry.key, leafEntry.value

	itr.prev()
	return key, value, true
}

// prev moves to the previous key. If no keys are before then depth is set to -1.
func (itr *SortedMapIterator[K, V]) prev() {
	for ; itr.depth >= 0; itr.depth-- {
		elem := &itr.stack[itr.depth]

		switch node := elem.node.(type) {
		case *sortedMapLeafNode[K, V]:
			if elem.index > 0 {
				elem.index--
				return
			}
		case *sortedMapBranchNode[K, V]:
			if elem.index > 0 {
				elem.index--
				itr.stack[itr.depth+1].node = node.elems[elem.index].node
				itr.depth++
				itr.last()
				return
			}
		}
	}
}

// first positions the stack to the leftmost key from the current depth.
// Elements and indexes below the current depth are assumed to be correct.
func (itr *SortedMapIterator[K, V]) first() {
	for {
		elem := &itr.stack[itr.depth]
		elem.index = 0

		switch node := elem.node.(type) {
		case *sortedMapBranchNode[K, V]:
			itr.stack[itr.depth+1] = sortedMapIteratorElem[K, V]{node: node.elems[elem.index].node}
			itr.depth++
		case *sortedMapLeafNode[K, V]:
			return
		}
	}
}

// last positions the stack to the rightmost key from the current depth.
// Elements and indexes below the current depth are assumed to be correct.
func (itr *SortedMapIterator[K, V]) last() {
	for {
		elem := &itr.stack[itr.depth]

		switch node := elem.node.(type) {
		case *sortedMapBranchNode[K, V]:
			elem.index = len(node.elems) - 1
			itr.stack[itr.depth+1] = sortedMapIteratorElem[K, V]{node: node.elems[elem.index].node}
			itr.depth++
		case *sortedMapLeafNode[K, V]:
			elem.index = len(node.entries) - 1
			return
		}
	}
}

// seek positions the stack to the given key from the current depth.
// Elements and indexes below the current depth are assumed to be correct.
func (itr *SortedMapIterator[K, V]) seek(key K) {
	for {
		elem := &itr.stack[itr.depth]
		elem.index = elem.node.indexOf(key, itr.m.comparer)

		switch node := elem.node.(type) {
		case *sortedMapBranchNode[K, V]:
			itr.stack[itr.depth+1] = sortedMapIteratorElem[K, V]{node: node.elems[elem.index].node}
			itr.depth++
		case *sortedMapLeafNode[K, V]:
			if elem.index == len(node.entries) {
				itr.next()
			}
			return
		}
	}
}

// sortedMapIteratorElem represents node/index pair in the SortedMapIterator stack.
type sortedMapIteratorElem[K constraints.Ordered, V any] struct {
	node  sortedMapNode[K, V]
	index int
}

// Comparer allows the comparison of two keys for the purpose of sorting.
type Comparer[K constraints.Ordered] interface {
	// Returns -1 if a is less than b, returns 1 if a is greater than b,
	// and returns 0 if a is equal to b.
	Compare(a, b K) int
}

// NewComparer returns the built-in comparer for a given key type.
func NewComparer[K constraints.Ordered](key K) Comparer[K] {
	// Attempt to use non-reflection based comparer first.
	switch (any(key)).(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, string:
		return &defaultComparer[K]{}
	}
	// Fallback to reflection-based comparer otherwise.
	// This is used when caller wraps a type around a primitive type.
	switch reflect.TypeOf(key).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.String:
		return &reflectComparer[K]{}
	}
	// If no comparers match then panic.
	// This is a compile time issue so it should not return an error.
	panic(fmt.Sprintf("immutable.NewComparer: must set comparer for %T type", key))
}

// defaultComparer compares two integers. Implements Comparer.
type defaultComparer[K constraints.Ordered] struct{}

// Compare returns -1 if a is less than b, returns 1 if a is greater than b, and
// returns 0 if a is equal to b. Panic if a or b is not an int.
func (c *defaultComparer[K]) Compare(i K, j K) int {
	if i < j {
		return -1
	} else if i > j {
		return 1
	}
	return 0
}

// reflectIntComparer compares two int values using reflection. Implements Comparer.
type reflectComparer[K constraints.Ordered] struct{}

// Compare returns -1 if a is less than b, returns 1 if a is greater than b, and
// returns 0 if a is equal to b. Panic if a or b is not an int.
func (c *reflectComparer[K]) Compare(a, b K) int {
	switch reflect.TypeOf(a).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if i, j := reflect.ValueOf(a).Int(), reflect.ValueOf(b).Int(); i < j {
			return -1
		} else if i > j {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if i, j := reflect.ValueOf(a).Uint(), reflect.ValueOf(b).Uint(); i < j {
			return -1
		} else if i > j {
			return 1
		}
		return 0
	case reflect.String:
		return strings.Compare(reflect.ValueOf(a).String(), reflect.ValueOf(b).String())
	}
	panic(fmt.Sprintf("immutable.reflectComparer.Compare: must set comparer for %T type", a))
}

func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
