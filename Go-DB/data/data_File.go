package data

import (
	"Go-DB/IO/fio"
	"fmt"
	"path/filepath"
)

const DataFileNameSuffix = ".data"
const DataHintFileNameSuffix = ".hint"
const MergeFinishFileName = ".MergeFinish"

//----------一个数据文件的结构
type DataFile struct {
	FileID    uint64
	Writeroff uint64
	IOManager fio.IOManager
}

func OpenDataFile(dirpath string, FileID uint64) (*DataFile, error) {
	FilleName := GetFileName(dirpath)
	return NewDataFile(FilleName, FileID)
}

func (df *DataFile) Write([]byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

func OpenHintFile(dir string) (*DataFile, error) {
	FileName := filepath.Join(dir, DataHintFileNameSuffix)
	DataFile, err := NewDataFile(FileName, 0)
	return DataFile, err

}

func (df *DataFile) WriteHintLogcord(key []byte, Pos *LogRecordpos) error {
	Record := LogRecord{
		Key:   key,
		Value: EnccodeLogRecordPos(Pos),
		Type:  0,
	}
	EncodeRecod, _ := EncodeLogRecord(&Record)
	return df.Write(EncodeRecod)
}

func OpenMergeFinishFile(dir string) (*DataFile, error) {
	FileName := filepath.Join(dir, MergeFinishFileName)
	DataFile, err := NewDataFile(FileName, 0)
	return DataFile, err
}

func NewDataFile(FileName string, FileID uint64) (*DataFile, error) {
	//初始化一个IOManger接口
	IOManager, err := fio.NewFileIOManagen(FileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileID,
		0,
		IOManager,
	}, nil

}

func GetFileName(dirpath string) string {
	FileName := filepath.Join(dirpath, fmt.Sprintf("{FileID}")+DataFileNameSuffix)
	return FileName
}
