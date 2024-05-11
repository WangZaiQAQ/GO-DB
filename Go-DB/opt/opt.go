package opt

type IndexType = int8

const (
	BTree IndexType = iota
	LSM
	Hash
)

//数据库目录

var FileSize uint64 = 10000

type Options struct {
	DirPath string //数据库目录

	FileSize uint64

	SyncEveryWriteFlag bool

	IndexType IndexType
}

type WriteBatch struct {

	//一个事务的最大操作条数
	MaxNumber uint

	SyncWrite bool //每个事务结束后是否都进行持久化

}

func LoadOptions() Options {

	NewOptions := Options{
		"E/Go-project",
		10000,
		true,
		BTree,
	}

	return NewOptions
}
