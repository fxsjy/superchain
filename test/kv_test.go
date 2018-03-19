package main
import (
	"testing"
	"strconv"
	"superchain/storage"
)

func TestLeveldb(t *testing.T) {
	kv_store := storage.NewStorage(storage.StorageEngineLeveldb)
	err := kv_store.Open(`{"DiskPaths":["/tmp/disk1/", "/tmp/disk2/"]}`)
	if err != nil {
		panic(err)
	}
	for i:=0; i < 10; i++ {
		key := "key_" + strconv.Itoa(i)
		value := "value__" + strconv.Itoa(i)
		err := kv_store.Put([]byte(key), []byte(value))
		if err != nil {
			panic(err)
		}
	}			
	for i:=0; i < 10; i=i+2 {
		key := "key_" + strconv.Itoa(i)
		err :=  kv_store.Delete([]byte(key))
		if err != nil {
			panic(err)
		}
	}
	for i:=0; i < 10; i++ {
		key := "key_" + strconv.Itoa(i)
		err, value := kv_store.Get([]byte(key))
		if i % 2 == 0 {
			if err != storage.ErrNotFound {
				t.Error(key , " should be deleted")
			}
		} else {
			expected_value := "value__" + strconv.Itoa(i)
			if err != nil || string(value) != expected_value {
				t.Error("written lost,", key)	
			}
		}
	}	
			
}

func TestOpenFail(t *testing.T) {
	kv_store := storage.NewStorage(storage.StorageEngineLeveldb)
	err := kv_store.Open(`{"DiskPaths":["/tmp/NO_NO_NO/", "/tmp/disk2/"]}`)
	if err == nil {
		t.Error("should open fail")
	}
}

