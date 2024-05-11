package index

import (
	"Go-DB/data"
	"Go-DB/index/Btree"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordpos) bool

	Get(key []byte) *data.LogRecordpos

	Delete(key []byte) bool
}

type Item struct {
	Key []byte
	Pos *data.LogRecordpos //data中定义，描述数据在磁盘中的位置
}

func (ai Item) Less(than btree.Item) bool {
	b := than.(*Item)
	return bytes.Compare(ai.Key, b.Key) == -1
}

// IndexType 初始化索引
type IndexType = int8

const (
	BTree IndexType = iota
	LSM
	Hash
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTree:
		return Btree.NewBTree()
	case LSM:
		return nil //NewART()
	case Hash:
		return nil
	}

	return nil
}
