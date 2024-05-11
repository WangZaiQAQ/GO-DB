package data //存放用到的数据结构
import (
	"encoding/binary"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
	LogRecordTxnFin
)

//------内存索引结构，描述了数据在磁盘中的那个文件夹，在文件夹的哪个位置（偏移量）
type LogRecordpos struct {
	Fid    uint64 //表明数据所在的文件id
	Offset uint64 //表明数据所在文件的偏移量
}

//------向文件夹写入时传进去的结构体
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

//------对传入的结构体LogRecord进行编码的方法
//将LogRecord转化成字节数组存进硬盘
//+-------------------------------------------------------------------------------
// CRC校验值  |   type类型   |   key size   |   value size   |   key   |   value   |
//   4字节           1字节      变长（不确定）     变长（不确定）     变长        变长
//+--------------------------------------------------------------------------------

func EncodeLogRecord(LogRecord *LogRecord) ([]byte, uint64) {
	header := make([]byte, maxLogRecordHeaderSize)

	return nil, 0
}

//------根据偏移量读取数据
func (fd *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	return nil, 0, nil
}

//解码
func ParseLogRecordKey(key []byte) ([]byte, error) {
	return nil, nil
}

//LogRecordPos进行编码的方法
func EnccodeLogRecordPos(pos *LogRecordpos) []byte {
	Buf := make([]byte, binary.MaxVarintLen16)
	var index = 0
	index += binary.PutVarint(Buf[index:], int64(pos.Fid))
	index += binary.PutVarint(Buf[index:], int64(pos.Offset))
	return Buf[index:]
}

//LogRecordPos进行解码
func DecodeLogRecordPos(Buf []byte) *LogRecordpos {
	var index = 0
	FileID, n := binary.Varint(Buf[index:])
	index += n
	Offset, _ := binary.Varint(Buf[index:])
	return &LogRecordpos{
		uint64(FileID),
		uint64(Offset),
	}

}
