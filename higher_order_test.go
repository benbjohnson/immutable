package immutable

import "testing"

func TestHigherOrderList(t *testing.T) {
	l := NewList(1, 2, 3)

	t.Run("filter", func(t *testing.T) {
		f := l.Filter(func(i int) bool { return i > 1 })
		if f.Len() != 2 {
			t.Fatalf("Unexpected filter result. expected 2, received %d", f.Len())
		}
		itr := f.Iterator()
		_, item := itr.Next()
		if item != 2 {
			t.Fatalf("Unexpected filtered item. expected 2, received %d", item)
		}
		_, item = itr.Next()
		if item != 3 {
			t.Fatalf("Unexpected filtered item. expected 3, received %d", item)
		}
	})

	t.Run("each", func(t *testing.T) {
		j := 0
		l.Each(func(i int) { j += i })
		if j != 6 {
			t.Fatalf("Unexpected Each handling: %d", j)
		}
	})

	t.Run("map", func(t *testing.T) {
		m := l.Map(func(i int) int { return i + 1 })
		if m.Len() != 3 {
			t.Fatalf("Unexpected map result. expected 3, received %d", m.Len())
		}
		itr := m.Iterator()
		if _, item := itr.Next(); item != 2 {
			t.Fatalf("Unexpected filtered item. expected 1, received %d", item)
		}
		if _, item := itr.Next(); item != 3 {
			t.Fatalf("Unexpected filtered item. expected 2, received %d", item)
		}
		if _, item := itr.Next(); item != 4 {
			t.Fatalf("Unexpected filtered item. expected 3, received %d", item)
		}
	})
}
