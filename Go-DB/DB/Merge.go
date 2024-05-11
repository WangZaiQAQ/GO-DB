package DB

import (
	"Go-DB/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const MergeDirName = "-merge"

// Merge 清除无效数据，生成hint文件
func (db *DB) Merge() error {
	//如果没有活跃文件就直接返回
	if db.ActiveFile == nil {
		return nil
	}
	//如果已经在Merge，则直接返回
	db.Mu.Lock()
	if db.IsMergeing {
		db.Mu.Unlock()
		return data.ErrMergeIsProgress
	}
	db.IsMergeing = true
	defer func() {
		db.IsMergeing = false
	}()

	//持久化当前活跃文件,并将其转化为旧的数据文件
	if err := db.ActiveFile.Sync(); err != nil {
		db.Mu.Unlock()
		return err
	}
	db.oldFile[db.ActiveFile.FileID] = db.ActiveFile

	//打开新的一个活跃文件。merge过程中写入的数据都放在该活跃文件中
	if err := db.SetActiveDataFile(); err != nil {
		db.Mu.Unlock()
		return err
	}

	//记录没有参与本次merge的文件
	NoMergeFileID := db.ActiveFile.FileID
	//取出所有需要merge的文件,放在切片MergeFlie[]中,并排序
	var MergeFile []*data.DataFile
	for _, file := range db.oldFile {
		MergeFile = append(MergeFile, file)
	}
	db.Mu.Unlock()
	sort.Slice(MergeFile, func(i, j int) bool {
		return MergeFile[i].FileID < MergeFile[j].FileID
	})

	MergePath := db.GetMergePath()
	//如果已经存在-Merge文件，证明之前发生过merge，将上一次的删掉
	//先忽略
	if _, err := os.Stat(MergePath); err != nil {
		err := os.Remove(MergePath)
		if err != nil {
			return err
		}
	}

	//新建一个mergepath目录
	if err := os.MkdirAll(MergePath, os.ModePerm); err != nil {
		return err
	}

	//打开一个临时的DB实例，
	MergeOptions := db.opt
	MergeOptions.DirPath = MergePath
	MergeOptions.SyncEveryWriteFlag = false
	MergeDB, err := Open(MergeOptions.DirPath)
	if err != nil {
		return err
	}

	//先打开一个hint文件用于存储索引
	HintFile, err := data.OpenHintFile(MergeOptions.DirPath)
	if err != nil {
		return err
	}

	//遍历每个数据文件
	for _, DataFile := range MergeFile {
		var Offset uint64 = 0
		//遍历数据文件中的每个KV
		for {
			LogRecord, Size, err := DataFile.ReadLogRecord(Offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//对拿到的东西解码得到key
			RealKey, _ := data.ParseLogRecordKey(LogRecord.Key)

			//拿到内存中该key对应的索引信息
			LogRecordPos := db.Index.Get(RealKey)

			//如果该条数据在内存索引中是存在的话，则重写入
			if LogRecordPos != nil && LogRecordPos.Fid == DataFile.FileID && LogRecordPos.Offset == Offset {
				//清除事务标记
				//LogRecord.Key = LogRecordKeyWithSeq()
				//调用追加写方法，对该条正确数据重写
				Pos, err := MergeDB.AppendLogRecord(LogRecord)
				if err != nil {
					return err
				}
				//将当前索引信息更新到hint文件
				if err := HintFile.WriteHintLogcord(RealKey, Pos); err != nil {
					return err
				}

			}
			//递增offset，指向下一条K-V
			Offset = +Size
		}
	}
	//持久化，保证数据全部写入到磁盘当中了
	if err := HintFile.Sync(); err != nil {
		return err
	}
	if err := MergeDB.ActiveFile.Sync(); err != nil {
		return err
	}
	//写标识，表示本次merge已经完成
	MergeFinishFile, err := data.OpenMergeFinishFile(MergeOptions.DirPath)
	if err != nil {
		return err
	}

	//写记录日志，表明本次Merge到哪一个文件
	MergeFinishRecord := &data.LogRecord{
		Key:   []byte("This Merge is Finish!"),
		Value: []byte(strconv.Itoa(int(NoMergeFileID))),
		Type:  0,
	}
	EncRecord, _ := data.EncodeLogRecord(MergeFinishRecord)
	if err := MergeFinishFile.Write(EncRecord); err != nil {
		return err
	}
	return nil
}

// GetMergePath ======拿到merge的目录
func (db *DB) GetMergePath() string {
	Dir := path.Dir(path.Clean(db.opt.DirPath)) //拿到db的父级目录
	Base := path.Base(db.opt.DirPath)
	return filepath.Join(Dir, Base+MergeDirName)
}

// LoadMergeFile 启动一次merge时对目录进行处理
func (db *DB) LoadMergeFile() error {
	MergePath := db.GetMergePath()

	//Merge目录不存在则直接返回

	DirEntrise, err := os.ReadDir(MergePath)
	if err != nil {
		return err
	}

	//查询"Merge是否完成的标识"，判断merge是否处理完了
	var MergeFinishedFlag bool
	var MergeFileName []string
	for _, entry := range DirEntrise {
		if entry.Name() == data.MergeFinishFileName {
			MergeFinishedFlag = true
		}
		MergeFileName = append(MergeFileName, entry.Name())
	}
	if MergeFinishedFlag == false {
		return nil
	}

	//拿到最近没有参与merge的文件ID

	//删除对应的数据文件（删除比active-file-id更小的文件）
	NonMergeFileID, err := db.GetNoMergeFile_min_ID(MergePath)
	if err != nil {
		return err
	}
	var FileID uint64 = 0
	for ; FileID < NonMergeFileID; FileID++ {
		FileName := data.GetFileName(db.opt.DirPath)
		if _, err := os.Stat(FileName); err == nil {
			if err := os.Remove(FileName); err != nil {
				return err
			}
		}
	}

	//将Merge目录中新的数据文件移动过到DB的目录
	for _, FileName := range MergeFileName {
		srcpath := filepath.Join(MergePath, FileName)
		destpath := filepath.Join(db.opt.DirPath, FileName)
		if err := os.Rename(srcpath, destpath); err != nil {
			return err
		}
	}

	return nil

}

func (db *DB) GetNoMergeFile_min_ID(Dir string) (uint64, error) {

	MergeFinishFile, err := data.OpenMergeFinishFile(Dir)
	if err != nil {
		return 0, err
	}

	record, _, err := MergeFinishFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	//字符串转化为整数
	NoMergeFile_min_ID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint64(NoMergeFile_min_ID), nil
}

//由hint文件加载索引
func (db *DB) LoadIndxFormHintFile() error {

	//查看索引文件是否存在
	HintFileName := filepath.Join(db.opt.DirPath, data.DataHintFileNameSuffix)
	if _, err := os.Stat(HintFileName); os.IsNotExist(err) {
		return err
	}

	//打开hint索引文件
	HintFile, err := data.OpenHintFile(db.opt.DirPath)
	if err != nil {
		return err
	}

	//读取文件中的索引
	var Offset uint64 = 0
	for {
		LogRecordpos, Size, err := HintFile.ReadLogRecord(Offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		//解码
		Pos := data.DecodeLogRecordPos(LogRecordpos.Value)
		//放到内存中
		db.Index.Put(LogRecordpos.Key, Pos)
		//更新偏移量，拿到下一条KV
		Offset = Offset + Size

	}

	return nil

}
