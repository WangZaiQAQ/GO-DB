package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManagen(FileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		FileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

//读
func (fio *FileIO) read(b []byte, offset int64) (int, error) {

	return fio.fd.ReadAt(b, offset)
}

//写
func (fio *FileIO) writer(b []byte) (int, error) {
	return fio.fd.Write(b)
}

//数据持久化
func (fio *FileIO) sync() error {
	return fio.fd.Sync()
}

//关闭文件
func (fio *FileIO) cloer() error {
	return fio.fd.Close()
}
