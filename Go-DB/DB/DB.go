package DB

import (
	"Go-DB/data"
	"Go-DB/index"
	"Go-DB/opt"
	"sync"
)

type DB struct {
	opt        opt.Options
	Mu         *sync.RWMutex
	ActiveFile *data.DataFile
	oldFile    map[uint64]*data.DataFile
	Index      index.Indexer
	SeqNo      uint64
	FileIDs    []int //加载索引的时候使用
	IsMergeing bool
}

func (db *DB) put(key []byte, value []byte) error {
	if len(key) == 0 {
		return data.ErrKeyIsEmpty //自定义的error
	}
	//	新建一个用于写入的结构体
	log_record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
		//需要一个墓碑值表示该变量是否在下一轮gc删除。
	}

	//调用apendLogRecord(),在磁盘中写入
	pos, err := db.AppendLogRecordUpLock(&log_record)
	if err != nil {
		return nil
	}
	//更新索引
	if ok := db.Index.Put(key, pos); !ok {
		return data.ErrIndexUpdataFailed //自定义error
	}
	return nil
}

// AppendLogRecordUpLock ------------上锁追加写
func (db *DB) AppendLogRecordUpLock(LogRecord *data.LogRecord) (*data.LogRecordpos, error) {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	return db.AppendLogRecord(LogRecord)

}

// AppendLogRecord ------------一般追加写
func (db *DB) AppendLogRecord(LogRecord *data.LogRecord) (*data.LogRecordpos, error) {

	//判断活跃文件（写入文件）是否存在，不存在就创建一个，一般第一次启动是没有的
	if db.ActiveFile == nil {
		if err := db.SetActiveDataFile(); err != nil {
			return nil, err
		}
	}
	//判断当前写入文件是否满了或放不下新的了，满了就新创建一个，
	encodeLogRecord, size := data.EncodeLogRecord(LogRecord)
	if db.ActiveFile.Writeroff+size > db.opt.FileSize {
		//持久化该旧文件夹
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}
		//将当前活跃文件转化为旧的数据文件
		db.oldFile[db.ActiveFile.FileID] = db.ActiveFile
		//创建一个活跃文件
		if err := db.SetActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//-------调用write（）写入磁盘中
	writeOff := db.ActiveFile.Writeroff
	if err := db.ActiveFile.Write(encodeLogRecord); err != nil {
		return nil, err
	}
	//用户通过配置文件决定，是否对新写入的数据进行一次持久化操作，
	if db.opt.SyncEveryWriteFlag == true {
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}
	}

	Pos := &data.LogRecordpos{
		Fid:    db.ActiveFile.FileID,
		Offset: writeOff,
	}

	return Pos, nil
}

// SetActiveDataFile 设置活跃文件夹
func (db *DB) SetActiveDataFile() error {

	var initActiveFileID uint64
	if db.ActiveFile != nil {
		initActiveFileID = db.ActiveFile.FileID + 1
	}

	datFile, err := data.OpenDataFile(db.opt.DirPath, initActiveFileID)
	if err != nil {
		return nil
	}
	db.ActiveFile = datFile
	return nil

}

// Get -------------------------------Get()
func (db *DB) Get(key []byte) ([]byte, error) {

	//判断key的合格性
	if len(key) == 0 {
		return nil, data.ErrKeyIsEmpty
	}

	//在索引中取出key对应的索引信息
	LogRecordpos := db.Index.Get(key)

	if LogRecordpos == nil {
		return nil, data.ErrKeyIsEmpty
	}

	//由索引信息找到数据文件
	var DataFile *data.DataFile
	if db.ActiveFile.FileID == LogRecordpos.Fid {
		DataFile = db.ActiveFile
	} else {
		DataFile = db.oldFile[LogRecordpos.Fid]
	}

	if DataFile == nil {
		return nil, data.ErrDataFileNotFound
	}

	//找到了，根据偏移量找到数据
	LogRecord, _, err := DataFile.ReadLogRecord(LogRecordpos.Offset)
	if err != nil {
		return nil, err
	}

	if LogRecord.Type == data.LogRecordDelete {
		return nil, data.ErrKeyIsNotFound
	}

	return LogRecord.Value, err

}
