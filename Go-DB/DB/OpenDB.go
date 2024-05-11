package DB

import (
	"Go-DB/data"
	"Go-DB/index"
	"Go-DB/opt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Open 打开一个Go-DB进程
func Open(dirPath string) (*DB, error) {

	//检验配置文件是否合格(暂略),并初始化配置文件
	Options := opt.LoadOptions()
	Options.DirPath = dirPath

	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB实例
	db := &DB{
		Options,
		new(sync.RWMutex),
		nil,
		make(map[uint64]*data.DataFile),
		index.NewIndexer(Options.IndexType),
		4,
		nil,
		false,
	}

	//加载merge文件

	//从hint文件中加载索引
	if err := db.LoadIndxFormHintFile(); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.LoadDtaFile(); err != nil {
		return nil, err
	}

	//加载索引
	if err := db.loadIndexesFromFile; err != nil {
		return nil, err()
	}

	return db, nil

}

//=====================================
//OpenDB()调用到的函数

// LoadDtaFile =========1.打开所有的数据文件
func (db *DB) LoadDtaFile() error {

	DirEntries, err := os.ReadDir(db.opt.DirPath)
	if err != nil {
		return err
	}

	var FileIDs []int
	for _, Entry := range DirEntries {
		if strings.HasSuffix(Entry.Name(), data.DataFileNameSuffix) {
			//分割文件名，拿到文件名，即文件ID
			SplitName := strings.Split(Entry.Name(), ".")
			FileID, err := strconv.Atoi(SplitName[0])
			if err != nil {
				return data.ErrDataDirCorrupted
			}
			FileIDs = append(FileIDs, FileID)
		}
	}
	//对存放所有文件ID的切片FileIDs进行排序
	sort.Ints(FileIDs)
	db.FileIDs = FileIDs

	//用一个for循环打开FileIDs中所有文件
	for i, fid := range FileIDs {
		DataFile, err := data.OpenDataFile(db.opt.DirPath, uint64(fid))
		if err != nil {
			return err
		}

		if i == len(FileIDs)-1 {
			db.ActiveFile = DataFile
		} else {
			db.oldFile[uint64(fid)] = DataFile
		}
	}
	return nil
}

//=========从数据文件中加载索引
func (db *DB) loadIndexesFromFile() error {

	//判断数据文件夹是否为空
	if len(db.FileIDs) == 0 {
		return nil
	}

	//拿个没发生过merge的文件的最小ID
	HasMerge, nonMergeFileID := false, uint64(0)
	MergeFinishFileName := filepath.Join(db.opt.DirPath, data.MergeFinishFileName)
	if _, err := os.Stat(MergeFinishFileName); err == nil {
		nonMergeFileID, err := db.GetNoMergeFile_min_ID(db.opt.DirPath)
		if err != nil {
			return err
		}
		HasMerge = true
		nonMergeFileID = fid

	}

	//遍历所有文件,
	for i, fid := range db.FileIDs {

		var FileID = uint64(fid)
		var DataFile *data.DataFile
		if FileID == db.ActiveFile.FileID {
			DataFile = db.ActiveFile
		} else {
			DataFile = db.oldFile[FileID]
		}

		var Offset uint64

		for {
			LogRecord, Size, err := DataFile.ReadLogRecord(Offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//构建索引结构并保存
			LogRecordpos := &data.LogRecordpos{Fid: FileID, Offset: Offset}
			if LogRecord.Type == data.LogRecordDelete {
				db.Index.Delete(LogRecord.Key)
			} else {
				db.Index.Put(LogRecord.Key, LogRecordpos)
			}
			//更新Offset
			Offset += Size

		}
		//
		if i == len(db.FileIDs)-1 {
			db.ActiveFile.Writeroff = Offset
		}
	}
	return nil
}
