package sturdyc

import (
	"math"
	"reflect"
)

// The constants below are rough approximations of the Go runtime's internal
// bookkeeping for maps. The exact numbers vary between Go versions, and the
// goal is an estimate that is in the right ballpark rather than an exact
// figure.
const (
	// mapHeaderSize approximates the size of the runtime's map header.
	mapHeaderSize = 48
	// mapSlotOverhead approximates the per-slot metadata (control/tophash byte).
	mapSlotOverhead = 1
)

var (
	stringHeaderSize = typeSize(reflect.TypeFor[string]())
	ptrSize          = typeSize(reflect.TypeFor[uintptr]())
)

// entrySizer estimates the number of bytes that a cache entry occupies.
type entrySizer[T any] struct {
	// baseSize is the fixed cost of every entry: the entry struct itself
	// (which includes the inline representation of T), and its slot in the
	// shard's map (the key's string header, the pointer to the entry, and
	// the map's per-slot metadata).
	baseSize int64
	// valueHasIndirectData is false for types such as ints, or structs of
	// ints, where the entire value is accounted for by the baseSize. For
	// those types we're able to skip the reflection walk entirely.
	valueHasIndirectData bool
}

func newEntrySizer[T any]() *entrySizer[T] {
	entrySize := typeSize(reflect.TypeFor[entry[T]]())
	return &entrySizer[T]{
		baseSize:             entrySize + stringHeaderSize + ptrSize + mapSlotOverhead,
		valueHasIndirectData: hasIndirectData(reflect.TypeFor[T]()),
	}
}

// size returns the estimated number of bytes that an entry with the given key
// and value occupies. It includes the bytes of the key, the value, and the
// cache's own per-entry bookkeeping. Entries that are larger than 2GiB are
// capped at math.MaxInt32, which is the maximum size that an entry can store.
func (s *entrySizer[T]) size(key string, value T) int32 {
	size := s.baseSize + int64(len(key))
	if s.valueHasIndirectData {
		size += indirectSizeOf(value)
	}
	//nolint:gosec // The size has been capped at math.MaxInt32, so it can't overflow.
	return int32(min(size, math.MaxInt32))
}

// indirectSizeOf is kept separate from the size method because taking the
// address of the value forces it to escape to the heap. Keeping it here means
// that values without any indirect data can skip that allocation.
func indirectSizeOf[T any](value T) int64 {
	// We're taking the address of the value so that interface types (e.g.
	// sturdyc.New[any]) are walked as interfaces, instead of as the concrete
	// type that they hold.
	var w sizeWalker
	return w.indirectSize(reflect.ValueOf(&value).Elem())
}

// sizeWalker uses reflection to estimate the memory that a value references,
// i.e. everything that isn't part of the value's inline representation. It
// keeps track of the pointers that it has visited so that shared and cyclic
// references are only counted once. Most values only contain a handful of
// pointers, so the first addresses are stored in an array, and we'll only
// switch to a map if a value has more pointers than the array can hold.
type sizeWalker struct {
	visited    [8]uintptr
	numVisited int
	seen       map[uintptr]struct{}
}

// firstVisit returns true the first time it's called with a given address.
func (w *sizeWalker) firstVisit(addr uintptr) bool {
	if w.seen != nil {
		if _, ok := w.seen[addr]; ok {
			return false
		}
		w.seen[addr] = struct{}{}
		return true
	}

	for i := 0; i < w.numVisited; i++ {
		if w.visited[i] == addr {
			return false
		}
	}

	if w.numVisited < len(w.visited) {
		w.visited[w.numVisited] = addr
		w.numVisited++
		return true
	}

	w.seen = make(map[uintptr]struct{}, 2*len(w.visited))
	for _, visited := range w.visited {
		w.seen[visited] = struct{}{}
	}
	w.seen[addr] = struct{}{}
	return true
}

func (w *sizeWalker) indirectSize(v reflect.Value) int64 {
	switch v.Kind() {
	case reflect.String:
		return int64(v.Len())
	case reflect.Pointer:
		return w.pointerSize(v)
	case reflect.Interface:
		return w.interfaceSize(v)
	case reflect.Slice:
		return w.sliceSize(v)
	case reflect.Array:
		return w.arraySize(v)
	case reflect.Struct:
		return w.structSize(v)
	case reflect.Map:
		return w.mapSize(v)
	case reflect.Invalid, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.Chan, reflect.Func, reflect.UnsafePointer:
		// These kinds either have no indirect data, or they reference
		// memory that we're unable to inspect.
		return 0
	}
	return 0
}

func (w *sizeWalker) pointerSize(v reflect.Value) int64 {
	if v.IsNil() || !w.firstVisit(v.Pointer()) {
		return 0
	}
	elem := v.Elem()
	return typeSize(elem.Type()) + w.indirectSize(elem)
}

func (w *sizeWalker) interfaceSize(v reflect.Value) int64 {
	if v.IsNil() {
		return 0
	}
	elem := v.Elem()
	// Pointer shaped values are stored directly in the interface, while
	// every other value is copied to a separate allocation.
	if isPointerShaped(elem.Kind()) {
		return w.indirectSize(elem)
	}
	return typeSize(elem.Type()) + w.indirectSize(elem)
}

func (w *sizeWalker) sliceSize(v reflect.Value) int64 {
	if v.IsNil() || v.Cap() == 0 || !w.firstVisit(v.Pointer()) {
		return 0
	}
	elemType := v.Type().Elem()
	size := int64(v.Cap()) * typeSize(elemType)
	if !hasIndirectData(elemType) {
		return size
	}
	for i := 0; i < v.Len(); i++ {
		size += w.indirectSize(v.Index(i))
	}
	return size
}

func (w *sizeWalker) arraySize(v reflect.Value) int64 {
	if !hasIndirectData(v.Type().Elem()) {
		return 0
	}
	var size int64
	for i := 0; i < v.Len(); i++ {
		size += w.indirectSize(v.Index(i))
	}
	return size
}

func (w *sizeWalker) structSize(v reflect.Value) int64 {
	var size int64
	for i := 0; i < v.NumField(); i++ {
		size += w.indirectSize(v.Field(i))
	}
	return size
}

func (w *sizeWalker) mapSize(v reflect.Value) int64 {
	if v.IsNil() || !w.firstVisit(v.Pointer()) {
		return 0
	}
	t := v.Type()
	slotSize := typeSize(t.Key()) + typeSize(t.Elem()) + mapSlotOverhead
	size := mapHeaderSize + int64(v.Len())*slotSize

	keyHasIndirectData := hasIndirectData(t.Key())
	elemHasIndirectData := hasIndirectData(t.Elem())
	if !keyHasIndirectData && !elemHasIndirectData {
		return size
	}

	iter := v.MapRange()
	for iter.Next() {
		if keyHasIndirectData {
			size += w.indirectSize(iter.Key())
		}
		if elemHasIndirectData {
			size += w.indirectSize(iter.Value())
		}
	}
	return size
}

// hasIndirectData reports whether values of the given type can reference
// memory outside of their inline representation.
func hasIndirectData(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.String, reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map:
		return true
	case reflect.Array:
		return t.Len() > 0 && hasIndirectData(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if hasIndirectData(t.Field(i).Type) {
				return true
			}
		}
		return false
	case reflect.Invalid, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return false
	}
	return false
}

// typeSize returns the size of the type's inline representation in bytes.
func typeSize(t reflect.Type) int64 {
	//nolint:gosec // The size of a type is bounded by the address space, so it can't overflow an int64.
	return int64(t.Size())
}

// isPointerShaped reports whether values of the given kind
// are stored directly in an interface's data word.
func isPointerShaped(k reflect.Kind) bool {
	return k == reflect.Pointer || k == reflect.Map || k == reflect.Chan ||
		k == reflect.Func || k == reflect.UnsafePointer
}
