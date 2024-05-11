package fio

type IOManager interface {

	//读
	read([]byte, int64) (int, error)

	//写
	writer([]byte) (int, error)

	//数据持久化
	sync() error

	//关闭文件
	cloer() error
}
