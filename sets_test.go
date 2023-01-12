package immutable

import (
	"testing"
)

func TestSetsPut(t *testing.T) {
	s := NewSet[string](nil)
	s2 := s.Add("1").Add("1")
	s2.Add("2")
	if s.Len() != 0 {
		t.Fatalf("Unexpected mutation of set")
	}
	if s.Has("1") {
		t.Fatalf("Unexpected set element")
	}
	if s2.Len() != 1 {
		t.Fatalf("Unexpected non-mutation of set")
	}
	if !s2.Has("1") {
		t.Fatalf("Set element missing")
	}
	itr := s2.Iterator()
	counter := 0
	for !itr.Done() {
		i, v := itr.Next()
		t.Log(i, v)
		counter++
	}
	if counter != 1 {
		t.Fatalf("iterator wrong length")
	}
}

func TestSetsDelete(t *testing.T) {
	s := NewSet[string](nil)
	s2 := s.Add("1")
	s3 := s.Delete("1")
	if s2.Len() != 1 {
		t.Fatalf("Unexpected non-mutation of set")
	}
	if !s2.Has("1") {
		t.Fatalf("Set element missing")
	}
	if s3.Len() != 0 {
		t.Fatalf("Unexpected set length after delete")
	}
	if s3.Has("1") {
		t.Fatalf("Unexpected set element after delete")
	}
}

func TestSortedSetsPut(t *testing.T) {
	s := NewSortedSet[string](nil)
	s2 := s.Add("1").Add("1").Add("0")
	if s.Len() != 0 {
		t.Fatalf("Unexpected mutation of set")
	}
	if s.Has("1") {
		t.Fatalf("Unexpected set element")
	}
	if s2.Len() != 2 {
		t.Fatalf("Unexpected non-mutation of set")
	}
	if !s2.Has("1") {
		t.Fatalf("Set element missing")
	}

	itr := s2.Iterator()
	counter := 0
	for !itr.Done() {
		i, v := itr.Next()
		t.Log(i, v)
		if counter == 0 && i != "0" {
			t.Fatalf("sort did not work for first el")
		}
		if counter == 1 && i != "1" {
			t.Fatalf("sort did not work for second el")
		}
		counter++
	}
	if counter != 2 {
		t.Fatalf("iterator wrong length")
	}
}

func TestSortedSetsDelete(t *testing.T) {
	s := NewSortedSet[string](nil)
	s2 := s.Add("1")
	s3 := s.Delete("1")
	if s2.Len() != 1 {
		t.Fatalf("Unexpected non-mutation of set")
	}
	if !s2.Has("1") {
		t.Fatalf("Set element missing")
	}
	if s3.Len() != 0 {
		t.Fatalf("Unexpected set length after delete")
	}
	if s3.Has("1") {
		t.Fatalf("Unexpected set element after delete")
	}
}
