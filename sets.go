package immutable

type Set[T comparable] struct {
	m *Map[T, struct{}]
}

func NewSet[T comparable](hasher Hasher[T], values ...T) Set[T] {
	s := Set[T]{
		m: NewMap[T, struct{}](hasher),
	}
	for _, value := range values {
		s.m.set(value, struct{}{}, true)
	}
	return s
}

func (s Set[T]) Set(values ...T) Set[T] {
	n := Set[T]{
		m: s.m.clone(),
	}
	for _, value := range values {
		n.m.set(value, struct{}{}, true)
	}
	return n
}

func (s Set[T]) Delete(values ...T) Set[T] {
	n := Set[T]{
		m: s.m.clone(),
	}
	for _, value := range values {
		n.m.delete(value, true)
	}
	return n
}

func (s Set[T]) Has(val T) bool {
	_, ok := s.m.Get(val)
	return ok
}

func (s Set[K]) Len() int {
	return s.m.Len()
}

func (s Set[T]) Iterator() *SetIterator[T] {
	itr := &SetIterator[T]{mi: s.m.Iterator()}
	itr.mi.First()
	return itr
}

type SetIterator[T comparable] struct {
	mi *MapIterator[T, struct{}]
}

func (itr *SetIterator[T]) Done() bool {
	return itr.mi.Done()
}

func (itr *SetIterator[T]) First() {
	itr.mi.First()
}

func (itr *SetIterator[T]) Next() (val T, ok bool) {
	val, _, ok = itr.mi.Next()
	return
}

type SetBuilder[T comparable] struct {
	s Set[T]
}

func NewSetBuilder[T comparable](hasher Hasher[T]) *SetBuilder[T] {
	return &SetBuilder[T]{s: NewSet(hasher)}
}

func (s SetBuilder[T]) Set(val T) {
	s.s.m = s.s.m.set(val, struct{}{}, true)
}

func (s SetBuilder[T]) Delete(val T) {
	s.s.m = s.s.m.delete(val, true)
}

func (s SetBuilder[T]) Has(val T) bool {
	return s.s.Has(val)
}

func (s SetBuilder[T]) Len() int {
	return s.s.Len()
}

type SortedSet[T comparable] struct {
	m *SortedMap[T, struct{}]
}

func NewSortedSet[T comparable](comparer Comparer[T], values ...T) SortedSet[T] {
	s := SortedSet[T]{
		m: NewSortedMap[T, struct{}](comparer),
	}
	for _, value := range values {
		s.m.set(value, struct{}{}, true)
	}
	return s
}

func (s SortedSet[T]) Set(values ...T) SortedSet[T] {
	n := SortedSet[T]{
		m: s.m.clone(),
	}
	for _, value := range values {
		n.m.set(value, struct{}{}, true)
	}
	return n
}

func (s SortedSet[T]) Delete(values ...T) SortedSet[T] {
	n := SortedSet[T]{
		m: s.m.clone(),
	}
	for _, value := range values {
		n.m.delete(value, true)
	}
	return n
}

func (s SortedSet[T]) Has(val T) bool {
	_, ok := s.m.Get(val)
	return ok
}

func (s SortedSet[K]) Len() int {
	return s.m.Len()
}

func (s SortedSet[T]) Iterator() *SortedSetIterator[T] {
	itr := &SortedSetIterator[T]{mi: s.m.Iterator()}
	itr.mi.First()
	return itr
}

type SortedSetIterator[T comparable] struct {
	mi *SortedMapIterator[T, struct{}]
}

func (itr *SortedSetIterator[T]) Done() bool {
	return itr.mi.Done()
}

func (itr *SortedSetIterator[T]) First() {
	itr.mi.First()
}

func (itr *SortedSetIterator[T]) Last() {
	itr.mi.Last()
}

func (itr *SortedSetIterator[T]) Next() (val T, ok bool) {
	val, _, ok = itr.mi.Next()
	return
}

func (itr *SortedSetIterator[T]) Prev() (val T, ok bool) {
	val, _, ok = itr.mi.Prev()
	return
}

func (itr *SortedSetIterator[T]) Seek(val T) {
	itr.mi.Seek(val)
}

type SortedSetBuilder[T comparable] struct {
	s SortedSet[T]
}

func NewSortedSetBuilder[T comparable](comparer Comparer[T]) *SortedSetBuilder[T] {
	return &SortedSetBuilder[T]{s: NewSortedSet(comparer)}
}

func (s SortedSetBuilder[T]) Set(val T) {
	s.s.m = s.s.m.set(val, struct{}{}, true)
}

func (s SortedSetBuilder[T]) Delete(val T) {
	s.s.m = s.s.m.delete(val, true)
}

func (s SortedSetBuilder[T]) Has(val T) bool {
	return s.s.Has(val)
}

func (s SortedSetBuilder[T]) Len() int {
	return s.s.Len()
}
