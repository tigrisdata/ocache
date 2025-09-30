package metadata

import (
	"os"
	"testing"

	grocksdb "github.com/linxGnu/grocksdb"
)

func TestNewMetaDB(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil) // nil merge operator for test
	if err != nil {
		t.Fatalf("NewMetaDB failed: %v", err)
	}
	if db == nil {
		t.Fatal("NewMetaDB returned nil")
	}
	if db.handle == nil {
		t.Fatal("MetaDB handle is nil")
	}

	expectedPath := diskPath + "/rocksdb"
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("RocksDB directory was not created")
	}

	db.Close()
}

func TestMetaDB_Handle(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	handle := db.Handle()
	if handle == nil {
		t.Fatal("Handle() returned nil")
	}

	db.Close()
}

func TestMetaDB_BasicOperations(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()

	key := []byte("test-key")
	value := []byte("test-value")

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err = handle.Put(wo, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	gotValue, err := handle.Get(ro, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer gotValue.Free()

	if !gotValue.Exists() {
		t.Error("Value should exist")
	}

	if string(gotValue.Data()) != string(value) {
		t.Errorf("Value mismatch: got %q, want %q", gotValue.Data(), value)
	}

	err = handle.Delete(wo, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	gotValue2, err := handle.Get(ro, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	defer gotValue2.Free()

	if gotValue2.Exists() {
		t.Error("Value should not exist after delete")
	}
}

func TestMetaDB_MultiplePutGet(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()

	numKeys := 10
	for i := 0; i < numKeys; i++ {
		key := []byte(string(rune('a' + i)))
		value := []byte(string(rune('A' + i)))

		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		err := handle.Put(wo, key, value)
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	for i := 0; i < numKeys; i++ {
		key := []byte(string(rune('a' + i)))
		expectedValue := []byte(string(rune('A' + i)))

		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		gotValue, err := handle.Get(ro, key)
		if err != nil {
			t.Fatalf("Get %d failed: %v", i, err)
		}
		defer gotValue.Free()

		if !gotValue.Exists() {
			t.Errorf("Key %d should exist", i)
		}

		if string(gotValue.Data()) != string(expectedValue) {
			t.Errorf("Value %d mismatch: got %q, want %q", i, gotValue.Data(), expectedValue)
		}
	}
}

func TestMetaDB_Overwrite(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()

	key := []byte("overwrite-key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err = handle.Put(wo, key, value1)
	if err != nil {
		t.Fatal(err)
	}

	err = handle.Put(wo, key, value2)
	if err != nil {
		t.Fatal(err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	gotValue, err := handle.Get(ro, key)
	if err != nil {
		t.Fatal(err)
	}
	defer gotValue.Free()

	if string(gotValue.Data()) != string(value2) {
		t.Errorf("Value should be overwritten: got %q, want %q", gotValue.Data(), value2)
	}
}

func TestMetaDB_EmptyKey(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()

	key := []byte("")
	value := []byte("empty-key-value")

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err = handle.Put(wo, key, value)
	if err != nil {
		t.Fatal(err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	gotValue, err := handle.Get(ro, key)
	if err != nil {
		t.Fatal(err)
	}
	defer gotValue.Free()

	if !gotValue.Exists() {
		t.Error("Empty key should be allowed")
	}

	if string(gotValue.Data()) != string(value) {
		t.Errorf("Value mismatch for empty key: got %q, want %q", gotValue.Data(), value)
	}
}

func TestMetaDB_LargeValue(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()

	key := []byte("large-value-key")
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err = handle.Put(wo, key, largeValue)
	if err != nil {
		t.Fatal(err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	gotValue, err := handle.Get(ro, key)
	if err != nil {
		t.Fatal(err)
	}
	defer gotValue.Free()

	if !gotValue.Exists() {
		t.Error("Large value should exist")
	}

	if len(gotValue.Data()) != len(largeValue) {
		t.Errorf("Large value size mismatch: got %d, want %d", len(gotValue.Data()), len(largeValue))
	}

	for i := 0; i < len(largeValue); i++ {
		if gotValue.Data()[i] != largeValue[i] {
			t.Errorf("Large value byte mismatch at position %d", i)
			break
		}
	}
}

func TestMetaDB_Persistence(t *testing.T) {
	diskPath := t.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	handle := db.Handle()

	key := []byte("persistent-key")
	value := []byte("persistent-value")

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err = handle.Put(wo, key, value)
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	handle2 := db2.Handle()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	gotValue, err := handle2.Get(ro, key)
	if err != nil {
		t.Fatal(err)
	}
	defer gotValue.Free()

	if !gotValue.Exists() {
		t.Error("Value should persist after reopening")
	}

	if string(gotValue.Data()) != string(value) {
		t.Errorf("Persisted value mismatch: got %q, want %q", gotValue.Data(), value)
	}
}

func BenchmarkMetaDB_Put(b *testing.B) {
	diskPath := b.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()
	value := []byte("benchmark-value")

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(string(rune(i)))
		err := handle.Put(wo, key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetaDB_Get(b *testing.B) {
	diskPath := b.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()

	numKeys := 1000
	value := []byte("benchmark-value")
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for i := 0; i < numKeys; i++ {
		key := []byte(string(rune(i)))
		handle.Put(wo, key, value)
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(string(rune(i % numKeys)))
		gotValue, err := handle.Get(ro, key)
		if err != nil {
			b.Fatal(err)
		}
		gotValue.Free()
	}
}

func BenchmarkMetaDB_Delete(b *testing.B) {
	diskPath := b.TempDir()
	ttl := 3600

	db, err := NewMetaDB(diskPath, ttl, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	handle := db.Handle()
	value := []byte("benchmark-value")

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for i := 0; i < b.N; i++ {
		key := []byte(string(rune(i)))
		handle.Put(wo, key, value)
	}

	wo2 := grocksdb.NewDefaultWriteOptions()
	defer wo2.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(string(rune(i)))
		err := handle.Delete(wo2, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
