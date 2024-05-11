package Btree

import (
	"Go-DB/data"
	"Go-DB/index"
	"github.com/google/btree"
	"sync"
)

//----------用BTree实现了memory-interface接口（内存索引方法）
type BTree struct {
	tree *btree.BTree
	lock sync.RWMutex
}

func (bt BTree) Put(key []byte, pos *data.LogRecordpos) bool {

	it := &index.Item{Key: key, Pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()

	return true
}
func (bt BTree) Get(key []byte) *data.LogRecordpos {

	it := &index.Item{Key: key}
	This_Item := bt.tree.Get(it)
	if This_Item == nil {
		return nil
	}
	return bt.Get(key)
}
func (bt BTree) Delete(key []byte) bool {
	it := &index.Item{Key: key}
	bt.lock.Lock()
	OldItem := bt.tree.Delete(it)
	if OldItem == nil {
		return false
	}
	return true
}

func NewBTree() *BTree {
	var mutex sync.RWMutex
	return &BTree{
		tree: btree.New(32),
		lock: mutex,
	}
}
