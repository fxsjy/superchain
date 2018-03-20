package  storage

import (
	"errors"
)

type Iterator interface {
	Key() []byte
	Value() []byte
	Next() bool
	Error() error
	Release()
}

type KVStore interface {
	Open(json_config string) error
	Close() error	
	Put(key []byte, value []byte) error
	Get(key []byte) (error, []byte)	
	Delete(key []byte) error
	Scan(first_key []byte, last_key []byte) Iterator
}

var (
    ErrNotFound         = errors.New("key not found")
    // TODO
)

const (
	StorageEngineLeveldb = iota
	StorageEngineBOS 
	StorageEngineMola
)

func NewStorage(engine int) KVStore {
	if engine == StorageEngineLeveldb {
		return new(LeveldbKVStore)
	} else {
		panic("not implemented")
		return nil
	}
}

type JsonConfig struct {
	DiskPaths []string; //only need it when we use leveldb as storage engine		
	RemoteLoginUser string; //may be used when we use remote storage
	RemoteLoginToken string; //...
	RemoteHostname string;
	RemotePort uint16;
}

