package immutable

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var (
	veryVerbose = flag.Bool("vv", false, "very verbose")
	randomN     = flag.Int("random.n", 100, "number of RunRandom() iterations")
)

func TestList(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		if size := NewList().Len(); size != 0 {
			t.Fatalf("unexpected size: %d", size)
		}
	})

	t.Run("Shallow", func(t *testing.T) {
		list := NewList()
		list = list.Append("foo")
		if v := list.Get(0); v != "foo" {
			t.Fatalf("unexpected value: %v", v)
		}

		other := list.Append("bar")
		if v := other.Get(0); v != "foo" {
			t.Fatalf("unexpected value: %v", v)
		} else if v := other.Get(1); v != "bar" {
			t.Fatalf("unexpected value: %v", v)
		}

		if v := list.Len(); v != 1 {
			t.Fatalf("unexpected value: %v", v)
		}
	})

	t.Run("Deep", func(t *testing.T) {
		list := NewList()
		var array []int
		for i := 0; i < 100000; i++ {
			list = list.Append(i)
			array = append(array, i)
		}

		if got, exp := len(array), list.Len(); got != exp {
			t.Fatalf("List.Len()=%d, exp %d", got, exp)
		}
		for j := range array {
			if got, exp := list.Get(j).(int), array[j]; got != exp {
				t.Fatalf("%d. List.Get(%d)=%d, exp %d", len(array), j, got, exp)
			}
		}
	})

	t.Run("Set", func(t *testing.T) {
		list := NewList()
		list = list.Append("foo")
		list = list.Append("bar")

		if v := list.Get(0); v != "foo" {
			t.Fatalf("unexpected value: %v", v)
		}

		list = list.Set(0, "baz")
		if v := list.Get(0); v != "baz" {
			t.Fatalf("unexpected value: %v", v)
		} else if v := list.Get(1); v != "bar" {
			t.Fatalf("unexpected value: %v", v)
		}
	})

	t.Run("GetBelowRange", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l.Get(-1)
		}()
		if r != `immutable.List.Get: index -1 out of bounds` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	t.Run("GetAboveRange", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l.Get(1)
		}()
		if r != `immutable.List.Get: index 1 out of bounds` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	t.Run("SetOutOfRange", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l.Set(1, "bar")
		}()
		if r != `immutable.List.Set: index 1 out of bounds` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	t.Run("SliceStartOutOfRange", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l.Slice(2, 3)
		}()
		if r != `immutable.List.Slice: start index 2 out of bounds` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	t.Run("SliceEndOutOfRange", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l.Slice(1, 3)
		}()
		if r != `immutable.List.Slice: end index 3 out of bounds` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	t.Run("SliceInvalidIndex", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l = l.Append("bar")
			l.Slice(2, 1)
		}()
		if r != `immutable.List.Slice: invalid slice index: [2:1]` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	t.Run("SliceBeginning", func(t *testing.T) {
		l := NewList()
		l = l.Append("foo")
		l = l.Append("bar")
		l = l.Slice(1, 2)
		if got, exp := l.Len(), 1; got != exp {
			t.Fatalf("List.Len()=%d, exp %d", got, exp)
		} else if got, exp := l.Get(0), "bar"; got != exp {
			t.Fatalf("List.Get(0)=%v, exp %v", got, exp)
		}
	})

	t.Run("IteratorSeekOutOfBounds", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			l := NewList()
			l = l.Append("foo")
			l.Iterator().Seek(-1)
		}()
		if r != `immutable.ListIterator.Seek: index -1 out of bounds` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	RunRandom(t, "Random", func(t *testing.T, rand *rand.Rand) {
		l := NewTList()
		for i := 0; i < 100000; i++ {
			rnd := rand.Intn(70)
			switch {
			case rnd == 0: // slice
				start, end := l.ChooseSliceIndices(rand)
				l.Slice(start, end)
			case rnd < 10: // set
				if l.Len() > 0 {
					l.Set(l.ChooseIndex(rand), rand.Intn(10000))
				}
			case rnd < 30: // prepend
				l.Prepend(rand.Intn(10000))
			default: // append
				l.Append(rand.Intn(10000))
			}

			if err := l.Validate(); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

// TList represents a list that operates on a standard Go slice & immutable list.
type TList struct {
	im, prev *List
	std      []int
}

// NewTList returns a new instance of TList.
func NewTList() *TList {
	return &TList{
		im: NewList(),
	}
}

// Len returns the size of the list.
func (l *TList) Len() int {
	return len(l.std)
}

// ChooseIndex returns a randomly chosen, valid index from the standard slice.
func (l *TList) ChooseIndex(rand *rand.Rand) int {
	if len(l.std) == 0 {
		return -1
	}
	return rand.Intn(len(l.std))
}

// ChooseSliceIndices returns randomly chosen, valid indices for slicing.
func (l *TList) ChooseSliceIndices(rand *rand.Rand) (start, end int) {
	if len(l.std) == 0 {
		return 0, 0
	}
	start = rand.Intn(len(l.std))
	end = rand.Intn(len(l.std)-start) + start
	return start, end
}

// Append adds v to the end of slice and List.
func (l *TList) Append(v int) {
	l.prev = l.im
	l.im = l.im.Append(v)
	l.std = append(l.std, v)
}

// Prepend adds v to the beginning of the slice and List.
func (l *TList) Prepend(v int) {
	l.prev = l.im
	l.im = l.im.Prepend(v)
	l.std = append([]int{v}, l.std...)
}

// Set updates the value at index i to v in the slice and List.
func (l *TList) Set(i, v int) {
	l.prev = l.im
	l.im = l.im.Set(i, v)
	l.std[i] = v
}

// Slice contracts the slice and List to the range of start/end indices.
func (l *TList) Slice(start, end int) {
	l.prev = l.im
	l.im = l.im.Slice(start, end)
	l.std = l.std[start:end]
}

// Validate returns an error if the slice and List are different.
func (l *TList) Validate() error {
	if got, exp := len(l.std), l.im.Len(); got != exp {
		return fmt.Errorf("Len()=%v, expected %d", got, exp)
	}

	for i := range l.std {
		if got, exp := l.im.Get(i), l.std[i]; got != exp {
			return fmt.Errorf("Get(%d)=%v, expected %v", i, got, exp)
		}
	}

	if err := l.validateForwardIterator(); err != nil {
		return err
	} else if err := l.validateBackwardIterator(); err != nil {
		return err
	}
	return nil
}

func (l *TList) validateForwardIterator() error {
	itr := l.im.Iterator()
	for i := range l.std {
		if j, v := itr.Next(); i != j || l.std[i] != v {
			return fmt.Errorf("ListIterator.Next()=<%v,%v>, expected <%v,%v>", j, v, i, l.std[i])
		}

		done := i == len(l.std)-1
		if v := itr.Done(); v != done {
			return fmt.Errorf("ListIterator.Done()=%v, expected %v", v, done)
		}
	}
	if i, v := itr.Next(); i != -1 || v != nil {
		return fmt.Errorf("ListIterator.Next()=<%v,%v>, expected DONE", i, v)
	}
	return nil
}

func (l *TList) validateBackwardIterator() error {
	itr := l.im.Iterator()
	itr.Last()
	for i := len(l.std) - 1; i >= 0; i-- {
		if j, v := itr.Prev(); i != j || l.std[i] != v {
			return fmt.Errorf("ListIterator.Prev()=<%v,%v>, expected <%v,%v>", j, v, i, l.std[i])
		}

		done := i == 0
		if v := itr.Done(); v != done {
			return fmt.Errorf("ListIterator.Done()=%v, expected %v", v, done)
		}
	}
	if i, v := itr.Prev(); i != -1 || v != nil {
		return fmt.Errorf("ListIterator.Prev()=<%v,%v>, expected DONE", i, v)
	}
	return nil
}

func BenchmarkList_Append(b *testing.B) {
	b.ReportAllocs()
	l := NewList()
	for i := 0; i < b.N; i++ {
		l = l.Append(i)
	}
}

func BenchmarkList_Prepend(b *testing.B) {
	b.ReportAllocs()
	l := NewList()
	for i := 0; i < b.N; i++ {
		l = l.Prepend(i)
	}
}

func BenchmarkList_Set(b *testing.B) {
	const n = 10000

	l := NewList()
	for i := 0; i < 10000; i++ {
		l = l.Append(i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		l = l.Set(i%n, i*10)
	}
}

func BenchmarkList_Iterator(b *testing.B) {
	const n = 10000
	l := NewList()
	for i := 0; i < 10000; i++ {
		l = l.Append(i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("Forward", func(b *testing.B) {
		itr := l.Iterator()
		for i := 0; i < b.N; i++ {
			if i%n == 0 {
				itr.First()
			}
			itr.Next()
		}
	})

	b.Run("Reverse", func(b *testing.B) {
		itr := l.Iterator()
		for i := 0; i < b.N; i++ {
			if i%n == 0 {
				itr.Last()
			}
			itr.Prev()
		}
	})
}

func ExampleList_Append() {
	l := NewList()
	l = l.Append("foo")
	l = l.Append("bar")
	l = l.Append("baz")

	fmt.Println(l.Get(0))
	fmt.Println(l.Get(1))
	fmt.Println(l.Get(2))
	// Output:
	// foo
	// bar
	// baz
}

func ExampleList_Prepend() {
	l := NewList()
	l = l.Prepend("foo")
	l = l.Prepend("bar")
	l = l.Prepend("baz")

	fmt.Println(l.Get(0))
	fmt.Println(l.Get(1))
	fmt.Println(l.Get(2))
	// Output:
	// baz
	// bar
	// foo
}

func ExampleList_Set() {
	l := NewList()
	l = l.Append("foo")
	l = l.Append("bar")
	l = l.Set(1, "baz")

	fmt.Println(l.Get(0))
	fmt.Println(l.Get(1))
	// Output:
	// foo
	// baz
}

func ExampleList_Slice() {
	l := NewList()
	l = l.Append("foo")
	l = l.Append("bar")
	l = l.Append("baz")
	l = l.Slice(1, 3)

	fmt.Println(l.Get(0))
	fmt.Println(l.Get(1))
	// Output:
	// bar
	// baz
}

func ExampleList_Iterator() {
	l := NewList()
	l = l.Append("foo")
	l = l.Append("bar")
	l = l.Append("baz")

	itr := l.Iterator()
	for !itr.Done() {
		i, v := itr.Next()
		fmt.Println(i, v)
	}
	// Output:
	// 0 foo
	// 1 bar
	// 2 baz
}

func ExampleList_Iterator_reverse() {
	l := NewList()
	l = l.Append("foo")
	l = l.Append("bar")
	l = l.Append("baz")

	itr := l.Iterator()
	itr.Last()
	for !itr.Done() {
		i, v := itr.Prev()
		fmt.Println(i, v)
	}
	// Output:
	// 2 baz
	// 1 bar
	// 0 foo
}

// Ensure node can support overwrites as it expands.
func TestInternal_mapNode_Overwrite(t *testing.T) {
	const n = 1000
	var h intHasher
	var node mapNode = &mapArrayNode{}
	for i := 0; i < n; i++ {
		var resized bool
		node = node.set(i, i, 0, h.Hash(i), &h, &resized)
		if !resized {
			t.Fatal("expected resize")
		}

		// Overwrite every node.
		for j := 0; j <= i; j++ {
			var resized bool
			node = node.set(j, i*j, 0, h.Hash(j), &h, &resized)
			if resized {
				t.Fatalf("expected no resize: i=%d, j=%d", i, j)
			}
		}

		// Verify not found at each branch type.
		if _, ok := node.get(1000000, 0, h.Hash(1000000), &h); ok {
			t.Fatal("expected no value")
		}
	}

	// Verify all key/value pairs in map.
	for i := 0; i < n; i++ {
		if v, ok := node.get(i, 0, h.Hash(i), &h); !ok || v != i*(n-1) {
			t.Fatalf("get(%d)=<%v,%v>", i, v, ok)
		}
	}
}

func TestInternal_mapArrayNode(t *testing.T) {
	// Ensure 8 or fewer elements stays in an array node.
	t.Run("Append", func(t *testing.T) {
		var h intHasher
		n := &mapArrayNode{}
		for i := 0; i < 8; i++ {
			var resized bool
			n = n.set(i*10, i, 0, h.Hash(i*10), &h, &resized).(*mapArrayNode)
			if !resized {
				t.Fatal("expected resize")
			}

			for j := 0; j < i; j++ {
				if v, ok := n.get(j*10, 0, h.Hash(j*10), &h); !ok || v != j {
					t.Fatalf("get(%d)=<%v,%v>", j, v, ok)
				}
			}
		}
	})

	// Ensure 8 or fewer elements stays in an array node when inserted in reverse.
	t.Run("Prepend", func(t *testing.T) {
		var h intHasher
		n := &mapArrayNode{}
		for i := 7; i >= 0; i-- {
			var resized bool
			n = n.set(i*10, i, 0, h.Hash(i*10), &h, &resized).(*mapArrayNode)
			if !resized {
				t.Fatal("expected resize")
			}

			for j := i; j <= 7; j++ {
				if v, ok := n.get(j*10, 0, h.Hash(j*10), &h); !ok || v != j {
					t.Fatalf("get(%d)=<%v,%v>", j, v, ok)
				}
			}
		}
	})

	// Ensure array can transition between node types.
	t.Run("Expand", func(t *testing.T) {
		var h intHasher
		var n mapNode = &mapArrayNode{}
		for i := 0; i < 100; i++ {
			var resized bool
			n = n.set(i, i, 0, h.Hash(i), &h, &resized)
			if !resized {
				t.Fatal("expected resize")
			}

			for j := 0; j < i; j++ {
				if v, ok := n.get(j, 0, h.Hash(j), &h); !ok || v != j {
					t.Fatalf("get(%d)=<%v,%v>", j, v, ok)
				}
			}
		}
	})

	// Ensure deleting elements returns the correct new node.
	RunRandom(t, "Delete", func(t *testing.T, rand *rand.Rand) {
		var h intHasher
		var n mapNode = &mapArrayNode{}
		for i := 0; i < 8; i++ {
			var resized bool
			n = n.set(i*10, i, 0, h.Hash(i*10), &h, &resized)
		}

		for _, i := range rand.Perm(8) {
			n = n.delete(i*10, 0, h.Hash(i*10), &h)
		}
		if n != nil {
			t.Fatal("expected nil rand")
		}
	})
}

func TestInternal_mapValueNode(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		var h intHasher
		n := newMapValueNode(h.Hash(2), 2, 3)
		if v, ok := n.get(2, 0, h.Hash(2), &h); !ok {
			t.Fatal("expected ok")
		} else if v != 3 {
			t.Fatalf("unexpected value: %v", v)
		}
	})

	t.Run("KeyEqual", func(t *testing.T) {
		var h intHasher
		var resized bool
		n := newMapValueNode(h.Hash(2), 2, 3)
		other := n.set(2, 4, 0, h.Hash(2), &h, &resized).(*mapValueNode)
		if other == n {
			t.Fatal("expected new node")
		} else if got, exp := other.keyHash, h.Hash(2); got != exp {
			t.Fatalf("keyHash=%v, expected %v", got, exp)
		} else if got, exp := other.key, 2; got != exp {
			t.Fatalf("key=%v, expected %v", got, exp)
		} else if got, exp := other.value, 4; got != exp {
			t.Fatalf("value=%v, expected %v", got, exp)
		} else if resized {
			t.Fatal("unexpected resize")
		}
	})

	t.Run("KeyHashEqual", func(t *testing.T) {
		h := &mockHasher{
			hash:  func(value interface{}) uint32 { return 1 },
			equal: func(a, b interface{}) bool { return a.(int) == b.(int) },
		}
		var resized bool
		n := newMapValueNode(h.Hash(2), 2, 3)
		other := n.set(4, 5, 0, h.Hash(4), h, &resized).(*mapHashCollisionNode)
		if got, exp := other.keyHash, h.Hash(2); got != exp {
			t.Fatalf("keyHash=%v, expected %v", got, exp)
		} else if got, exp := len(other.entries), 2; got != exp {
			t.Fatalf("entries=%v, expected %v", got, exp)
		} else if !resized {
			t.Fatal("expected resize")
		}
		if got, exp := other.entries[0].key, 2; got != exp {
			t.Fatalf("key[0]=%v, expected %v", got, exp)
		} else if got, exp := other.entries[0].value, 3; got != exp {
			t.Fatalf("value[0]=%v, expected %v", got, exp)
		}
		if got, exp := other.entries[1].key, 4; got != exp {
			t.Fatalf("key[1]=%v, expected %v", got, exp)
		} else if got, exp := other.entries[1].value, 5; got != exp {
			t.Fatalf("value[1]=%v, expected %v", got, exp)
		}
	})

	t.Run("MergeNode", func(t *testing.T) {
		// Inserting into a node with a different index in the mask should split into a bitmap node.
		t.Run("NoConflict", func(t *testing.T) {
			var h intHasher
			var resized bool
			n := newMapValueNode(h.Hash(2), 2, 3)
			other := n.set(4, 5, 0, h.Hash(4), &h, &resized).(*mapBitmapIndexedNode)
			if got, exp := other.bitmap, uint32(0x14); got != exp {
				t.Fatalf("bitmap=0x%02x, expected 0x%02x", got, exp)
			} else if got, exp := len(other.nodes), 2; got != exp {
				t.Fatalf("nodes=%v, expected %v", got, exp)
			} else if !resized {
				t.Fatal("expected resize")
			}
			if node, ok := other.nodes[0].(*mapValueNode); !ok {
				t.Fatalf("node[0]=%T, unexpected type", other.nodes[0])
			} else if got, exp := node.key, 2; got != exp {
				t.Fatalf("key[0]=%v, expected %v", got, exp)
			} else if got, exp := node.value, 3; got != exp {
				t.Fatalf("value[0]=%v, expected %v", got, exp)
			}
			if node, ok := other.nodes[1].(*mapValueNode); !ok {
				t.Fatalf("node[1]=%T, unexpected type", other.nodes[1])
			} else if got, exp := node.key, 4; got != exp {
				t.Fatalf("key[1]=%v, expected %v", got, exp)
			} else if got, exp := node.value, 5; got != exp {
				t.Fatalf("value[1]=%v, expected %v", got, exp)
			}

			// Ensure both values can be read.
			if v, ok := other.get(2, 0, h.Hash(2), &h); !ok || v.(int) != 3 {
				t.Fatalf("Get(2)=<%v,%v>", v, ok)
			} else if v, ok := other.get(4, 0, h.Hash(4), &h); !ok || v.(int) != 5 {
				t.Fatalf("Get(4)=<%v,%v>", v, ok)
			}
		})

		// Reversing the nodes from NoConflict should yield the same result.
		t.Run("NoConflictReverse", func(t *testing.T) {
			var h intHasher
			var resized bool
			n := newMapValueNode(h.Hash(4), 4, 5)
			other := n.set(2, 3, 0, h.Hash(2), &h, &resized).(*mapBitmapIndexedNode)
			if got, exp := other.bitmap, uint32(0x14); got != exp {
				t.Fatalf("bitmap=0x%02x, expected 0x%02x", got, exp)
			} else if got, exp := len(other.nodes), 2; got != exp {
				t.Fatalf("nodes=%v, expected %v", got, exp)
			} else if !resized {
				t.Fatal("expected resize")
			}
			if node, ok := other.nodes[0].(*mapValueNode); !ok {
				t.Fatalf("node[0]=%T, unexpected type", other.nodes[0])
			} else if got, exp := node.key, 2; got != exp {
				t.Fatalf("key[0]=%v, expected %v", got, exp)
			} else if got, exp := node.value, 3; got != exp {
				t.Fatalf("value[0]=%v, expected %v", got, exp)
			}
			if node, ok := other.nodes[1].(*mapValueNode); !ok {
				t.Fatalf("node[1]=%T, unexpected type", other.nodes[1])
			} else if got, exp := node.key, 4; got != exp {
				t.Fatalf("key[1]=%v, expected %v", got, exp)
			} else if got, exp := node.value, 5; got != exp {
				t.Fatalf("value[1]=%v, expected %v", got, exp)
			}

			// Ensure both values can be read.
			if v, ok := other.get(2, 0, h.Hash(2), &h); !ok || v.(int) != 3 {
				t.Fatalf("Get(2)=<%v,%v>", v, ok)
			} else if v, ok := other.get(4, 0, h.Hash(4), &h); !ok || v.(int) != 5 {
				t.Fatalf("Get(4)=<%v,%v>", v, ok)
			}
		})

		// Inserting a node with the same mask index should nest an additional level of bitmap nodes.
		t.Run("Conflict", func(t *testing.T) {
			h := &mockHasher{
				hash:  func(value interface{}) uint32 { return uint32(value.(int)) << 5 },
				equal: func(a, b interface{}) bool { return a.(int) == b.(int) },
			}
			var resized bool
			n := newMapValueNode(h.Hash(2), 2, 3)
			other := n.set(4, 5, 0, h.Hash(4), h, &resized).(*mapBitmapIndexedNode)
			if got, exp := other.bitmap, uint32(0x01); got != exp { // mask is zero, expect first slot.
				t.Fatalf("bitmap=0x%02x, expected 0x%02x", got, exp)
			} else if got, exp := len(other.nodes), 1; got != exp {
				t.Fatalf("nodes=%v, expected %v", got, exp)
			} else if !resized {
				t.Fatal("expected resize")
			}
			child, ok := other.nodes[0].(*mapBitmapIndexedNode)
			if !ok {
				t.Fatalf("node[0]=%T, unexpected type", other.nodes[0])
			}

			if node, ok := child.nodes[0].(*mapValueNode); !ok {
				t.Fatalf("node[0]=%T, unexpected type", child.nodes[0])
			} else if got, exp := node.key, 2; got != exp {
				t.Fatalf("key[0]=%v, expected %v", got, exp)
			} else if got, exp := node.value, 3; got != exp {
				t.Fatalf("value[0]=%v, expected %v", got, exp)
			}
			if node, ok := child.nodes[1].(*mapValueNode); !ok {
				t.Fatalf("node[1]=%T, unexpected type", child.nodes[1])
			} else if got, exp := node.key, 4; got != exp {
				t.Fatalf("key[1]=%v, expected %v", got, exp)
			} else if got, exp := node.value, 5; got != exp {
				t.Fatalf("value[1]=%v, expected %v", got, exp)
			}

			// Ensure both values can be read.
			if v, ok := other.get(2, 0, h.Hash(2), h); !ok || v.(int) != 3 {
				t.Fatalf("Get(2)=<%v,%v>", v, ok)
			} else if v, ok := other.get(4, 0, h.Hash(4), h); !ok || v.(int) != 5 {
				t.Fatalf("Get(4)=<%v,%v>", v, ok)
			} else if v, ok := other.get(10, 0, h.Hash(10), h); ok {
				t.Fatalf("Get(10)=<%v,%v>, expected no value", v, ok)
			}
		})
	})
}

func TestMap_Get(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		m := NewMap(nil)
		if v, ok := m.Get(100); ok || v != nil {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
	})
}

func TestMap_Set(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		m := NewMap(nil)
		itr := m.Iterator()
		if !itr.Done() {
			t.Fatal("MapIterator.Done()=true, expected false")
		} else if k, v := itr.Next(); k != nil || v != nil {
			t.Fatalf("MapIterator.Next()=<%v,%v>, expected nil", k, v)
		}
	})

	t.Run("Simple", func(t *testing.T) {
		m := NewMap(nil)
		m = m.Set(100, "foo")
		if v, ok := m.Get(100); !ok || v != "foo" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
	})

	t.Run("VerySmall", func(t *testing.T) {
		const n = 6
		m := NewMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}

		// NOTE: Array nodes store entries in insertion order.
		itr := m.Iterator()
		for i := 0; i < n; i++ {
			if k, v := itr.Next(); k != i || v != i+1 {
				t.Fatalf("MapIterator.Next()=<%v,%v>, exp <%v,%v>", k, v, i, i+1)
			}
		}
		if !itr.Done() {
			t.Fatal("expected iterator done")
		}
	})

	t.Run("Small", func(t *testing.T) {
		const n = 1000
		m := NewMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}
	})

	t.Run("Large", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping: short")
		}

		const n = 1000000
		m := NewMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}
	})

	t.Run("StringKeys", func(t *testing.T) {
		m := NewMap(nil)
		m = m.Set("foo", "bar")
		m = m.Set("baz", "bat")
		m = m.Set("", "EMPTY")
		if v, ok := m.Get("foo"); !ok || v != "bar" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get("baz"); !ok || v != "bat" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get(""); !ok || v != "EMPTY" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
		if v, ok := m.Get("no_such_key"); ok {
			t.Fatalf("expected no value: <%v,%v>", v, ok)
		}
	})

	t.Run("ByteSliceKeys", func(t *testing.T) {
		m := NewMap(nil)
		m = m.Set([]byte("foo"), "bar")
		m = m.Set([]byte("baz"), "bat")
		m = m.Set([]byte(""), "EMPTY")
		if v, ok := m.Get([]byte("foo")); !ok || v != "bar" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get([]byte("baz")); !ok || v != "bat" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get([]byte("")); !ok || v != "EMPTY" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
		if v, ok := m.Get([]byte("no_such_key")); ok {
			t.Fatalf("expected no value: <%v,%v>", v, ok)
		}
	})

	t.Run("NoDefaultHasher", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			m := NewMap(nil)
			m = m.Set(uint64(100), "bar")
		}()
		if r != `immutable.Map.Set: must set hasher for uint64 type` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	RunRandom(t, "Random", func(t *testing.T, rand *rand.Rand) {
		m := NewTestMap()
		for i := 0; i < 100000; i++ {
			switch rand.Intn(2) {
			case 1: // overwrite
				m.Set(m.ExistingKey(rand), rand.Intn(10000))
			default: // set new key
				m.Set(m.NewKey(rand), rand.Intn(10000))
			}
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

// Ensure map can support overwrites as it expands.
func TestMap_Overwrite(t *testing.T) {
	const n = 10000
	m := NewMap(nil)
	for i := 0; i < n; i++ {
		// Set original value.
		m = m.Set(i, i)

		// Overwrite every node.
		for j := 0; j <= i; j++ {
			m = m.Set(j, i*j)
		}
	}

	// Verify all key/value pairs in map.
	for i := 0; i < n; i++ {
		if v, ok := m.Get(i); !ok || v != i*(n-1) {
			t.Fatalf("Get(%d)=<%v,%v>", i, v, ok)
		}
	}
}

func TestMap_Delete(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		m := NewMap(nil)
		other := m.Delete("foo")
		if m != other {
			t.Fatal("expected same map")
		}
	})

	t.Run("Simple", func(t *testing.T) {
		m := NewMap(nil)
		m = m.Set(100, "foo")
		if v, ok := m.Get(100); !ok || v != "foo" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
	})

	t.Run("Small", func(t *testing.T) {
		const n = 1000
		m := NewMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := range rand.New(rand.NewSource(0)).Perm(n) {
			m = m.Delete(i)
		}
		if m.Len() != 0 {
			t.Fatalf("expected no elements, got %d", m.Len())
		}
	})

	t.Run("Large", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping: short")
		}
		const n = 1000000
		m := NewMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := range rand.New(rand.NewSource(0)).Perm(n) {
			m = m.Delete(i)
		}
		if m.Len() != 0 {
			t.Fatalf("expected no elements, got %d", m.Len())
		}
	})

	RunRandom(t, "Random", func(t *testing.T, rand *rand.Rand) {
		m := NewTestMap()
		for i := 0; i < 100000; i++ {
			switch rand.Intn(8) {
			case 0: // overwrite
				m.Set(m.ExistingKey(rand), rand.Intn(10000))
			case 1: // delete existing key
				m.Delete(m.ExistingKey(rand))
			case 2: // delete non-existent key.
				m.Delete(m.NewKey(rand))
			default: // set new key
				m.Set(m.NewKey(rand), rand.Intn(10000))
			}
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}

		// Delete all and verify they are gone.
		keys := make([]int, len(m.keys))
		copy(keys, m.keys)

		for _, key := range keys {
			m.Delete(key)
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

// Ensure map works even with hash conflicts.
func TestMap_LimitedHash(t *testing.T) {
	h := mockHasher{
		hash:  func(value interface{}) uint32 { return hashUint64(uint64(value.(int))) % 0xFF },
		equal: func(a, b interface{}) bool { return a.(int) == b.(int) },
	}
	m := NewMap(&h)

	rand := rand.New(rand.NewSource(0))
	keys := rand.Perm(100000)
	for _, i := range keys {
		m = m.Set(i, i) // initial set
	}
	for i := range keys {
		m = m.Set(i, i*2) // overwrite
	}
	if m.Len() != len(keys) {
		t.Fatalf("unexpected len: %d", m.Len())
	}

	// Verify all key/value pairs in map.
	for i := 0; i < m.Len(); i++ {
		if v, ok := m.Get(i); !ok || v != i*2 {
			t.Fatalf("Get(%d)=<%v,%v>", i, v, ok)
		}
	}

	// Verify iteration.
	itr := m.Iterator()
	for !itr.Done() {
		if k, v := itr.Next(); v != k.(int)*2 {
			t.Fatalf("MapIterator.Next()=<%v,%v>, expected value %v", k, v, k.(int)*2)
		}
	}

	// Verify not found works.
	if _, ok := m.Get(10000000); ok {
		t.Fatal("expected no value")
	}

	// Verify delete non-existent key works.
	if other := m.Delete(10000000 + 1); m != other {
		t.Fatal("expected no change")
	}

	// Remove all keys.
	for _, key := range keys {
		m = m.Delete(key)
	}
	if m.Len() != 0 {
		t.Fatalf("unexpected size: %d", m.Len())
	}
}

// TestMap represents a combined immutable and stdlib map.
type TestMap struct {
	im, prev *Map
	std      map[int]int
	keys     []int
}

func NewTestMap() *TestMap {
	return &TestMap{
		im:  NewMap(nil),
		std: make(map[int]int),
	}
}

func (m *TestMap) NewKey(rand *rand.Rand) int {
	for {
		k := rand.Int()
		if _, ok := m.std[k]; !ok {
			return k
		}
	}
}

func (m *TestMap) ExistingKey(rand *rand.Rand) int {
	if len(m.keys) == 0 {
		return 0
	}
	return m.keys[rand.Intn(len(m.keys))]
}

func (m *TestMap) Set(k, v int) {
	m.prev = m.im
	m.im = m.im.Set(k, v)

	_, exists := m.std[k]
	if !exists {
		m.keys = append(m.keys, k)
	}
	m.std[k] = v
}

func (m *TestMap) Delete(k int) {
	m.prev = m.im
	m.im = m.im.Delete(k)
	delete(m.std, k)

	for i := range m.keys {
		if m.keys[i] == k {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

func (m *TestMap) Validate() error {
	for _, k := range m.keys {
		if v, ok := m.im.Get(k); !ok {
			return fmt.Errorf("key not found: %d", k)
		} else if v != m.std[k] {
			return fmt.Errorf("key (%d) mismatch: immutable=%d, std=%d", k, v, m.std[k])
		}
	}
	return m.validateIterator()
}

func (m *TestMap) validateIterator() error {
	other := make(map[int]int)
	itr := m.im.Iterator()
	for !itr.Done() {
		k, v := itr.Next()
		other[k.(int)] = v.(int)
	}
	if diff := cmp.Diff(other, m.std); diff != "" {
		return fmt.Errorf("map iterator mismatch: %s", diff)
	}
	if k, v := itr.Next(); k != nil || v != nil {
		return fmt.Errorf("map iterator returned key/value after done: <%v/%v>", k, v)
	}
	return nil
}

func BenchmarkMap_Set(b *testing.B) {
	b.ReportAllocs()
	m := NewMap(nil)
	for i := 0; i < b.N; i++ {
		m = m.Set(i, i)
	}
}

func BenchmarkMap_Delete(b *testing.B) {
	const n = 10000

	m := NewMap(nil)
	for i := 0; i < n; i++ {
		m = m.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Delete(i % n) // Do not update map, always operate on original
	}
}

func BenchmarkMap_Iterator(b *testing.B) {
	const n = 10000
	m := NewMap(nil)
	for i := 0; i < 10000; i++ {
		m = m.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("Forward", func(b *testing.B) {
		itr := m.Iterator()
		for i := 0; i < b.N; i++ {
			if i%n == 0 {
				itr.First()
			}
			itr.Next()
		}
	})
}

func ExampleMap_Set() {
	m := NewMap(nil)
	m = m.Set("foo", "bar")
	m = m.Set("baz", 100)

	v, ok := m.Get("foo")
	fmt.Println("foo", v, ok)

	v, ok = m.Get("baz")
	fmt.Println("baz", v, ok)

	v, ok = m.Get("bat") // does not exist
	fmt.Println("bat", v, ok)
	// Output:
	// foo bar true
	// baz 100 true
	// bat <nil> false
}

func ExampleMap_Delete() {
	m := NewMap(nil)
	m = m.Set("foo", "bar")
	m = m.Set("baz", 100)
	m = m.Delete("baz")

	v, ok := m.Get("foo")
	fmt.Println("foo", v, ok)

	v, ok = m.Get("baz")
	fmt.Println("baz", v, ok)
	// Output:
	// foo bar true
	// baz <nil> false
}

func ExampleMap_Iterator() {
	m := NewMap(nil)
	m = m.Set("apple", 100)
	m = m.Set("grape", 200)
	m = m.Set("kiwi", 300)
	m = m.Set("mango", 400)
	m = m.Set("orange", 500)
	m = m.Set("peach", 600)
	m = m.Set("pear", 700)
	m = m.Set("pineapple", 800)
	m = m.Set("strawberry", 900)

	itr := m.Iterator()
	for !itr.Done() {
		k, v := itr.Next()
		fmt.Println(k, v)
	}
	// Output:
	// mango 400
	// pear 700
	// pineapple 800
	// grape 200
	// orange 500
	// strawberry 900
	// kiwi 300
	// peach 600
	// apple 100
}

func TestInternalSortedMapLeafNode(t *testing.T) {
	RunRandom(t, "NoSplit", func(t *testing.T, rand *rand.Rand) {
		var cmpr intComparer
		var node sortedMapNode = &sortedMapLeafNode{}
		var keys []int
		for _, i := range rand.Perm(32) {
			var resized bool
			var splitNode sortedMapNode
			node, splitNode = node.set(i, i*10, &cmpr, &resized)
			if !resized {
				t.Fatal("expected resize")
			} else if splitNode != nil {
				t.Fatal("expected split")
			}
			keys = append(keys, i)

			// Verify not found at each size.
			if _, ok := node.get(rand.Int()+32, &cmpr); ok {
				t.Fatal("expected no value")
			}

			// Verify min key is always the lowest.
			sort.Ints(keys)
			if got, exp := node.minKey(), keys[0]; got != exp {
				t.Fatalf("minKey()=%d, expected %d", got, exp)
			}
		}

		// Verify all key/value pairs in node.
		for i := range keys {
			if v, ok := node.get(i, &cmpr); !ok || v != i*10 {
				t.Fatalf("get(%d)=<%v,%v>", i, v, ok)
			}
		}
	})

	RunRandom(t, "Overwrite", func(t *testing.T, rand *rand.Rand) {
		var cmpr intComparer
		var node sortedMapNode = &sortedMapLeafNode{}
		for _, i := range rand.Perm(32) {
			var resized bool
			node, _ = node.set(i, i*2, &cmpr, &resized)
		}
		for _, i := range rand.Perm(32) {
			var resized bool
			node, _ = node.set(i, i*3, &cmpr, &resized)
			if resized {
				t.Fatal("expected no resize")
			}
		}

		// Verify all overwritten key/value pairs in node.
		for i := 0; i < 32; i++ {
			if v, ok := node.get(i, &cmpr); !ok || v != i*3 {
				t.Fatalf("get(%d)=<%v,%v>", i, v, ok)
			}
		}
	})

	t.Run("Split", func(t *testing.T) {
		// Fill leaf node.
		var cmpr intComparer
		var node sortedMapNode = &sortedMapLeafNode{}
		for i := 0; i < 32; i++ {
			var resized bool
			node, _ = node.set(i, i*10, &cmpr, &resized)
		}

		// Add one more and expect split.
		var resized bool
		newNode, splitNode := node.set(32, 320, &cmpr, &resized)

		// Verify node contents.
		newLeafNode, ok := newNode.(*sortedMapLeafNode)
		if !ok {
			t.Fatalf("unexpected node type: %T", newLeafNode)
		} else if n := len(newLeafNode.entries); n != 16 {
			t.Fatalf("unexpected node len: %d", n)
		}
		for i := range newLeafNode.entries {
			if entry := newLeafNode.entries[i]; entry.key != i || entry.value != i*10 {
				t.Fatalf("%d. unexpected entry: %v=%v", i, entry.key, entry.value)
			}
		}

		// Verify split node contents.
		splitLeafNode, ok := splitNode.(*sortedMapLeafNode)
		if !ok {
			t.Fatalf("unexpected split node type: %T", splitLeafNode)
		} else if n := len(splitLeafNode.entries); n != 17 {
			t.Fatalf("unexpected split node len: %d", n)
		}
		for i := range splitLeafNode.entries {
			if entry := splitLeafNode.entries[i]; entry.key != (i+16) || entry.value != (i+16)*10 {
				t.Fatalf("%d. unexpected split node entry: %v=%v", i, entry.key, entry.value)
			}
		}
	})
}

func TestInternalSortedMapBranchNode(t *testing.T) {
	RunRandom(t, "NoSplit", func(t *testing.T, rand *rand.Rand) {
		keys := make([]int, 32*16)
		for i := range keys {
			keys[i] = rand.Intn(10000)
		}
		keys = uniqueIntSlice(keys)
		sort.Ints(keys[:2]) // ensure first two keys are sorted for initial insert.

		// Initialize branch with two leafs.
		var cmpr intComparer
		leaf0 := &sortedMapLeafNode{entries: []mapEntry{{key: keys[0], value: keys[0] * 10}}}
		leaf1 := &sortedMapLeafNode{entries: []mapEntry{{key: keys[1], value: keys[1] * 10}}}
		var node sortedMapNode = newSortedMapBranchNode(leaf0, leaf1)

		sort.Ints(keys)
		for _, i := range rand.Perm(len(keys)) {
			key := keys[i]

			var resized bool
			var splitNode sortedMapNode
			node, splitNode = node.set(key, key*10, &cmpr, &resized)
			if key == leaf0.entries[0].key || key == leaf1.entries[0].key {
				if resized {
					t.Fatalf("expected no resize: key=%d", key)
				}
			} else {
				if !resized {
					t.Fatalf("expected resize: key=%d", key)
				}
			}
			if splitNode != nil {
				t.Fatal("unexpected split")
			}
		}

		// Verify all key/value pairs in node.
		for _, key := range keys {
			if v, ok := node.get(key, &cmpr); !ok || v != key*10 {
				t.Fatalf("get(%d)=<%v,%v>", key, v, ok)
			}
		}

		// Verify min key is the lowest key.
		if got, exp := node.minKey(), keys[0]; got != exp {
			t.Fatalf("minKey()=%d, expected %d", got, exp)
		}
	})

	t.Run("Split", func(t *testing.T) {
		// Generate leaf nodes.
		var cmpr intComparer
		children := make([]sortedMapNode, 32)
		for i := range children {
			leaf := &sortedMapLeafNode{entries: make([]mapEntry, 32)}
			for j := range leaf.entries {
				leaf.entries[j] = mapEntry{key: (i * 32) + j, value: ((i * 32) + j) * 100}
			}
			children[i] = leaf
		}
		var node sortedMapNode = newSortedMapBranchNode(children...)

		// Add one more and expect split.
		var resized bool
		newNode, splitNode := node.set((32 * 32), (32*32)*100, &cmpr, &resized)

		// Verify node contents.
		var idx int
		newBranchNode, ok := newNode.(*sortedMapBranchNode)
		if !ok {
			t.Fatalf("unexpected node type: %T", newBranchNode)
		} else if n := len(newBranchNode.elems); n != 16 {
			t.Fatalf("unexpected child elems len: %d", n)
		}
		for i, elem := range newBranchNode.elems {
			child, ok := elem.node.(*sortedMapLeafNode)
			if !ok {
				t.Fatalf("unexpected child type")
			}
			for j, entry := range child.entries {
				if entry.key != idx || entry.value != idx*100 {
					t.Fatalf("%d/%d. unexpected entry: %v=%v", i, j, entry.key, entry.value)
				}
				idx++
			}
		}

		// Verify split node contents.
		splitBranchNode, ok := splitNode.(*sortedMapBranchNode)
		if !ok {
			t.Fatalf("unexpected split node type: %T", splitBranchNode)
		} else if n := len(splitBranchNode.elems); n != 17 {
			t.Fatalf("unexpected split node elem len: %d", n)
		}
		for i, elem := range splitBranchNode.elems {
			child, ok := elem.node.(*sortedMapLeafNode)
			if !ok {
				t.Fatalf("unexpected split node child type")
			}
			for j, entry := range child.entries {
				if entry.key != idx || entry.value != idx*100 {
					t.Fatalf("%d/%d. unexpected split node entry: %v=%v", i, j, entry.key, entry.value)
				}
				idx++
			}
		}
	})
}

func TestSortedMap_Get(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		m := NewSortedMap(nil)
		if v, ok := m.Get(100); ok || v != nil {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
	})
}

func TestSortedMap_Set(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		m := NewSortedMap(nil)
		m = m.Set(100, "foo")
		if v, ok := m.Get(100); !ok || v != "foo" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if got, exp := m.Len(), 1; got != exp {
			t.Fatalf("SortedMap.Len()=%d, exp %d", got, exp)
		}
	})

	t.Run("Small", func(t *testing.T) {
		const n = 1000
		m := NewSortedMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}
	})

	t.Run("Large", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping: short")
		}

		const n = 1000000
		m := NewSortedMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}
	})

	t.Run("StringKeys", func(t *testing.T) {
		m := NewSortedMap(nil)
		m = m.Set("foo", "bar")
		m = m.Set("baz", "bat")
		m = m.Set("", "EMPTY")
		if v, ok := m.Get("foo"); !ok || v != "bar" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get("baz"); !ok || v != "bat" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get(""); !ok || v != "EMPTY" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
		if v, ok := m.Get("no_such_key"); ok {
			t.Fatalf("expected no value: <%v,%v>", v, ok)
		}
	})

	t.Run("ByteSliceKeys", func(t *testing.T) {
		m := NewSortedMap(nil)
		m = m.Set([]byte("foo"), "bar")
		m = m.Set([]byte("baz"), "bat")
		m = m.Set([]byte(""), "EMPTY")
		if v, ok := m.Get([]byte("foo")); !ok || v != "bar" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get([]byte("baz")); !ok || v != "bat" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		} else if v, ok := m.Get([]byte("")); !ok || v != "EMPTY" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
		if v, ok := m.Get([]byte("no_such_key")); ok {
			t.Fatalf("expected no value: <%v,%v>", v, ok)
		}
	})

	t.Run("NoDefaultComparer", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			m := NewSortedMap(nil)
			m = m.Set(uint64(100), "bar")
		}()
		if r != `immutable.SortedMap.Set: must set comparer for uint64 type` {
			t.Fatalf("unexpected panic: %q", r)
		}
	})

	RunRandom(t, "Random", func(t *testing.T, rand *rand.Rand) {
		m := NewTestSortedMap()
		for j := 0; j < 10000; j++ {
			switch rand.Intn(2) {
			case 1: // overwrite
				m.Set(m.ExistingKey(rand), rand.Intn(10000))
			default: // set new key
				m.Set(m.NewKey(rand), rand.Intn(10000))
			}
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

// Ensure map can support overwrites as it expands.
func TestSortedMap_Overwrite(t *testing.T) {
	const n = 1000
	m := NewSortedMap(nil)
	for i := 0; i < n; i++ {
		// Set original value.
		m = m.Set(i, i)

		// Overwrite every node.
		for j := 0; j <= i; j++ {
			m = m.Set(j, i*j)
		}
	}

	// Verify all key/value pairs in map.
	for i := 0; i < n; i++ {
		if v, ok := m.Get(i); !ok || v != i*(n-1) {
			t.Fatalf("Get(%d)=<%v,%v>", i, v, ok)
		}
	}
}

func TestSortedMap_Delete(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		m := NewSortedMap(nil)
		m = m.Delete(100)
		if n := m.Len(); n != 0 {
			t.Fatalf("SortedMap.Len()=%d, expected 0", n)
		}
	})

	t.Run("Simple", func(t *testing.T) {
		m := NewSortedMap(nil)
		m = m.Set(100, "foo")
		if v, ok := m.Get(100); !ok || v != "foo" {
			t.Fatalf("unexpected value: <%v,%v>", v, ok)
		}
		m = m.Delete(100)
		if v, ok := m.Get(100); ok {
			t.Fatalf("unexpected no value: <%v,%v>", v, ok)
		}
	})

	t.Run("Small", func(t *testing.T) {
		const n = 1000
		m := NewSortedMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}

		for i := 0; i < n; i++ {
			m = m.Delete(i)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); ok {
				t.Fatalf("expected no value for key=%v: <%v,%v>", i, v, ok)
			}
		}
	})

	t.Run("Large", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping: short")
		}

		const n = 1000000
		m := NewSortedMap(nil)
		for i := 0; i < n; i++ {
			m = m.Set(i, i+1)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); !ok || v != i+1 {
				t.Fatalf("unexpected value for key=%v: <%v,%v>", i, v, ok)
			}
		}

		for i := 0; i < n; i++ {
			m = m.Delete(i)
		}
		for i := 0; i < n; i++ {
			if v, ok := m.Get(i); ok {
				t.Fatalf("unexpected no value for key=%v: <%v,%v>", i, v, ok)
			}
		}
	})

	RunRandom(t, "Random", func(t *testing.T, rand *rand.Rand) {
		m := NewTestSortedMap()
		for j := 0; j < 10000; j++ {
			switch rand.Intn(8) {
			case 0: // overwrite
				m.Set(m.ExistingKey(rand), rand.Intn(10000))
			case 1: // delete existing key
				m.Delete(m.ExistingKey(rand))
			case 2: // delete non-existent key.
				m.Delete(m.NewKey(rand))
			default: // set new key
				m.Set(m.NewKey(rand), rand.Intn(10000))
			}
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSortedMap_Iterator(t *testing.T) {
	t.Run("Seek", func(t *testing.T) {
		const n = 100
		m := NewSortedMap(nil)
		for i := 0; i < n; i += 2 {
			m = m.Set(fmt.Sprintf("%04d", i), i)
		}

		t.Run("Exact", func(t *testing.T) {
			itr := m.Iterator()
			for i := 0; i < n; i += 2 {
				itr.Seek(fmt.Sprintf("%04d", i))
				for j := i; j < n; j += 2 {
					if k, _ := itr.Next(); k != fmt.Sprintf("%04d", j) {
						t.Fatalf("%d/%d. SortedMapIterator.Next()=%v, expected key %04d", i, j, k, j)
					}
				}
				if !itr.Done() {
					t.Fatalf("SortedMapIterator.Done()=true, expected false")
				}
			}
		})

		t.Run("Miss", func(t *testing.T) {
			itr := m.Iterator()
			for i := 1; i < n-2; i += 2 {
				itr.Seek(fmt.Sprintf("%04d", i))
				for j := i + 1; j < n; j += 2 {
					if k, _ := itr.Next(); k != fmt.Sprintf("%04d", j) {
						t.Fatalf("%d/%d. SortedMapIterator.Next()=%v, expected key %04d", i, j, k, j)
					}
				}
				if !itr.Done() {
					t.Fatalf("SortedMapIterator.Done()=true, expected false")
				}
			}
		})

		t.Run("BeforeFirst", func(t *testing.T) {
			itr := m.Iterator()
			itr.Seek("")
			for i := 0; i < n; i += 2 {
				if k, _ := itr.Next(); k != fmt.Sprintf("%04d", i) {
					t.Fatalf("%d. SortedMapIterator.Next()=%v, expected key %04d", i, k, i)
				}
			}
			if !itr.Done() {
				t.Fatalf("SortedMapIterator.Done()=true, expected false")
			}
		})
		t.Run("AfterLast", func(t *testing.T) {
			itr := m.Iterator()
			itr.Seek("1000")
			if k, _ := itr.Next(); k != nil {
				t.Fatalf("0. SortedMapIterator.Next()=%v, expected nil key", k)
			} else if !itr.Done() {
				t.Fatalf("SortedMapIterator.Done()=true, expected false")
			}
		})
	})
}

// TestSortedMap represents a combined immutable and stdlib sorted map.
type TestSortedMap struct {
	im, prev *SortedMap
	std      map[int]int
	keys     []int
}

func NewTestSortedMap() *TestSortedMap {
	return &TestSortedMap{
		im:  NewSortedMap(nil),
		std: make(map[int]int),
	}
}

func (m *TestSortedMap) NewKey(rand *rand.Rand) int {
	for {
		k := rand.Int()
		if _, ok := m.std[k]; !ok {
			return k
		}
	}
}

func (m *TestSortedMap) ExistingKey(rand *rand.Rand) int {
	if len(m.keys) == 0 {
		return 0
	}
	return m.keys[rand.Intn(len(m.keys))]
}

func (m *TestSortedMap) Set(k, v int) {
	m.prev = m.im
	m.im = m.im.Set(k, v)

	if _, ok := m.std[k]; !ok {
		m.keys = append(m.keys, k)
		sort.Ints(m.keys)
	}
	m.std[k] = v
}

func (m *TestSortedMap) Delete(k int) {
	m.prev = m.im
	m.im = m.im.Delete(k)
	delete(m.std, k)

	for i := range m.keys {
		if m.keys[i] == k {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

func (m *TestSortedMap) Validate() error {
	for _, k := range m.keys {
		if v, ok := m.im.Get(k); !ok {
			return fmt.Errorf("key not found: %d", k)
		} else if v != m.std[k] {
			return fmt.Errorf("key (%d) mismatch: immutable=%d, std=%d", k, v, m.std[k])
		}
	}

	sort.Ints(m.keys)
	if err := m.validateForwardIterator(); err != nil {
		return err
	} else if err := m.validateBackwardIterator(); err != nil {
		return err
	}
	return nil
}

func (m *TestSortedMap) validateForwardIterator() error {
	itr := m.im.Iterator()
	for i, k0 := range m.keys {
		v0 := m.std[k0]
		if k1, v1 := itr.Next(); k0 != k1 || v0 != v1 {
			return fmt.Errorf("%d. SortedMapIterator.Next()=<%v,%v>, expected <%v,%v>", i, k1, v1, k0, v0)
		}

		done := i == len(m.keys)-1
		if v := itr.Done(); v != done {
			return fmt.Errorf("%d. SortedMapIterator.Done()=%v, expected %v", i, v, done)
		}
	}
	if k, v := itr.Next(); k != nil || v != nil {
		return fmt.Errorf("SortedMapIterator.Next()=<%v,%v>, expected nil after done", k, v)
	}
	return nil
}

func (m *TestSortedMap) validateBackwardIterator() error {
	itr := m.im.Iterator()
	itr.Last()
	for i := len(m.keys) - 1; i >= 0; i-- {
		k0 := m.keys[i]
		v0 := m.std[k0]
		if k1, v1 := itr.Prev(); k0 != k1 || v0 != v1 {
			return fmt.Errorf("%d. SortedMapIterator.Prev()=<%v,%v>, expected <%v,%v>", i, k1, v1, k0, v0)
		}

		done := i == 0
		if v := itr.Done(); v != done {
			return fmt.Errorf("%d. SortedMapIterator.Done()=%v, expected %v", i, v, done)
		}
	}
	if k, v := itr.Prev(); k != nil || v != nil {
		return fmt.Errorf("SortedMapIterator.Prev()=<%v,%v>, expected nil after done", k, v)
	}
	return nil
}

func BenchmarkSortedMap_Set(b *testing.B) {
	b.ReportAllocs()
	m := NewSortedMap(nil)
	for i := 0; i < b.N; i++ {
		m = m.Set(i, i)
	}
}

func BenchmarkSortedMap_Delete(b *testing.B) {
	const n = 10000

	m := NewSortedMap(nil)
	for i := 0; i < n; i++ {
		m = m.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Delete(i % n) // Do not update map, always operate on original
	}
}

func BenchmarkSortedMap_Iterator(b *testing.B) {
	const n = 10000
	m := NewSortedMap(nil)
	for i := 0; i < 10000; i++ {
		m = m.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("Forward", func(b *testing.B) {
		itr := m.Iterator()
		for i := 0; i < b.N; i++ {
			if i%n == 0 {
				itr.First()
			}
			itr.Next()
		}
	})

	b.Run("Reverse", func(b *testing.B) {
		itr := m.Iterator()
		for i := 0; i < b.N; i++ {
			if i%n == 0 {
				itr.Last()
			}
			itr.Prev()
		}
	})
}

func ExampleSortedMap_Set() {
	m := NewSortedMap(nil)
	m = m.Set("foo", "bar")
	m = m.Set("baz", 100)

	v, ok := m.Get("foo")
	fmt.Println("foo", v, ok)

	v, ok = m.Get("baz")
	fmt.Println("baz", v, ok)

	v, ok = m.Get("bat") // does not exist
	fmt.Println("bat", v, ok)
	// Output:
	// foo bar true
	// baz 100 true
	// bat <nil> false
}

func ExampleSortedMap_Delete() {
	m := NewSortedMap(nil)
	m = m.Set("foo", "bar")
	m = m.Set("baz", 100)
	m = m.Delete("baz")

	v, ok := m.Get("foo")
	fmt.Println("foo", v, ok)

	v, ok = m.Get("baz")
	fmt.Println("baz", v, ok)
	// Output:
	// foo bar true
	// baz <nil> false
}

func ExampleSortedMap_Iterator() {
	m := NewSortedMap(nil)
	m = m.Set("strawberry", 900)
	m = m.Set("kiwi", 300)
	m = m.Set("apple", 100)
	m = m.Set("pear", 700)
	m = m.Set("pineapple", 800)
	m = m.Set("peach", 600)
	m = m.Set("orange", 500)
	m = m.Set("grape", 200)
	m = m.Set("mango", 400)

	itr := m.Iterator()
	for !itr.Done() {
		k, v := itr.Next()
		fmt.Println(k, v)
	}
	// Output:
	// apple 100
	// grape 200
	// kiwi 300
	// mango 400
	// orange 500
	// peach 600
	// pear 700
	// pineapple 800
	// strawberry 900
}

// RunRandom executes fn multiple times with a different rand.
func RunRandom(t *testing.T, name string, fn func(t *testing.T, rand *rand.Rand)) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Run(name, func(t *testing.T) {
		for i := 0; i < *randomN; i++ {
			t.Run(fmt.Sprintf("%08d", i), func(t *testing.T) {
				t.Parallel()
				fn(t, rand.New(rand.NewSource(int64(i))))
			})
		}
	})
}

func uniqueIntSlice(a []int) []int {
	m := make(map[int]struct{})
	other := make([]int, 0, len(a))
	for _, v := range a {
		if _, ok := m[v]; ok {
			continue
		}
		m[v] = struct{}{}
		other = append(other, v)
	}
	return other
}

// mockHasher represents a mock implementation of immutable.Hasher.
type mockHasher struct {
	hash  func(value interface{}) uint32
	equal func(a, b interface{}) bool
}

// Hash executes the mocked HashFn function.
func (h *mockHasher) Hash(value interface{}) uint32 {
	return h.hash(value)
}

// Equal executes the mocked EqualFn function.
func (h *mockHasher) Equal(a, b interface{}) bool {
	return h.equal(a, b)
}

// mockComparer represents a mock implementation of immutable.Comparer.
type mockComparer struct {
	compare func(a, b interface{}) int
}

// Compare executes the mocked CompreFn function.
func (h *mockComparer) Compare(a, b interface{}) int {
	return h.compare(a, b)
}
