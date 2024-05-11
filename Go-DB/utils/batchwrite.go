package utils

import (
	"Go-DB/DB"
	"Go-DB/data"
	"Go-DB/opt"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

//字符串转字节数组
var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写操作
type WriteBatch struct {
	options       opt.WriteBatch
	mu            *sync.Mutex
	db            *DB.DB
	pendingWrites map[string]*data.LogRecord
}

// Put ------------写入的数据先暂存到map里
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	//key长度为0返回错误
	if len(key) == 0 {
		return data.ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	LogRecord := &data.LogRecord{Key: key, Value: value}

	wb.pendingWrites[string(key)] = LogRecord
	return nil
}

func (wb *WriteBatch) Commit() error {
	//加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//判断map是否为空，空则表示没有要提交的事务
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	//判断map的个数是否已经超过了配置项允许的事务最大操作数
	if uint(len(wb.pendingWrites)) > wb.options.MaxNumber {
		return data.ErrKeyIsEmpty
	}

	//加一把全局锁确保事务提交的串行化
	wb.db.Mu.Lock()
	wb.db.Mu.Unlock()

	//获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.SeqNo, 1)

	//-----开始写数据到数据文件中
	opsitions := make(map[string]*data.LogRecordpos) //保存的是索引信息
	for _, record := range wb.pendingWrites {
		LogRecordpos, err := wb.db.AppendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWhitSqeNo(record.Key, seqNo), //key和seqNo经过编码后得到的序列
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		opsitions[string(record.Key)] = LogRecordpos
	}

	//---//标识该事务已完成的数据，
	FinshedRecord := data.LogRecord{
		Key:  logRecordKeyWhitSqeNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFin,
	}
	if _, err := wb.db.AppendLogRecord(&FinshedRecord); err != nil {
		return err
	}

	//---//根据配置文件进行持久化
	if wb.options.SyncWrite && wb.db.ActiveFile != nil {
		err := wb.db.ActiveFile.Sync()
		if err != nil {
			return err
		}
	}

	//---//更新索引
	for _, record := range wb.pendingWrites {
		pos := opsitions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.Index.Put(record.Key, pos)
		}

		if record.Type == data.LogRecordDelete {
			wb.db.Index.Delete(record.Key)
		}
	}

	//---//清空暂存数据

	return nil
}

//将传入的 key 和 SeqNo 组合编码成一个新的字节数组，其中前部分是 SeqNo 的变长编码，后部分是原始的 key，以此来创建一个新的编码后的键。
func logRecordKeyWhitSqeNo(key []byte, SeqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen16)
	n := binary.PutUvarint(seq[:], SeqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}
