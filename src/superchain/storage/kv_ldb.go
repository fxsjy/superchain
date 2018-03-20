package storage

import (
	"hash/crc32"
	"encoding/json"
	"log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbKVStore struct {
	dbs []*leveldb.DB
}

type LeveldbUnorderIterator struct {
	iters []iterator.Iterator	
	cur_iter_index int
}

func (this *LeveldbUnorderIterator) AddIter(it iterator.Iterator) {
	this.iters = append(this.iters, it)
}

func (this *LeveldbUnorderIterator) Release() {
	for _, iter := range this.iters {
		iter.Release()
	}
}

func (this *LeveldbUnorderIterator) Error() error{
	return this.iters[this.cur_iter_index].Error() 	
}

func (this *LeveldbUnorderIterator) Key() []byte{
	return this.iters[this.cur_iter_index].Key()
}

func (this *LeveldbUnorderIterator) Value() []byte {
	return this.iters[this.cur_iter_index].Value()
}

func (this *LeveldbUnorderIterator) Next() bool {
	for x := this.cur_iter_index; x < len(this.iters); x++ {
		if this.iters[x].Next() {
			this.cur_iter_index = x
			return true
		}
	}
	return false;	
}

func (this *LeveldbKVStore) Open(json_config string) error {
	config := &JsonConfig{}
	json_err := json.Unmarshal([]byte(json_config), config)
	if json_err != nil {
		return json_err
	}
	for _, path := range config.DiskPaths {
		log.Println("try open:", path)
		db, err := leveldb.OpenFile(path, nil)
		if err != nil {
			return err
		}
		this.dbs = append(this.dbs, db) 
	}
	return nil
}

func (this *LeveldbKVStore) Close() error {
	var last_err error = nil
	for _, db_instance := range this.dbs {
		err := db_instance.Close()
		if nil != err {
			last_err = err
		}
	}
	return last_err
}

func (this *LeveldbKVStore) getBucket(key []byte) uint32 {
	key_hash := crc32.ChecksumIEEE(key)
	bucket := key_hash % uint32(len(this.dbs))
	return bucket
}

func (this *LeveldbKVStore) Put(key []byte, value []byte) error {
	bucket := this.getBucket(key)
	return this.dbs[bucket].Put(key, value, nil)
}

func (this *LeveldbKVStore) Get(key []byte) (error, []byte) {
	bucket := this.getBucket(key)
	value, err := this.dbs[bucket].Get(key, nil)
	if err == leveldb.ErrNotFound {
		err = ErrNotFound
	}
	return err, value
}
	
func (this *LeveldbKVStore) Delete(key []byte) error{
	bucket := this.getBucket(key)
	return this.dbs[bucket].Delete(key, nil)
}

func (this *LeveldbKVStore) Scan(first_key []byte, last_key []byte) Iterator {
	scan_iter := &LeveldbUnorderIterator{cur_iter_index:0}
	for _, db_instance := range this.dbs {
		one_iter := db_instance.NewIterator(&util.Range{Start: first_key, Limit: last_key}, nil)
		scan_iter.AddIter(one_iter)
	}
	return scan_iter
}

