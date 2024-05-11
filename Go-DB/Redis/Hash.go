package Redis

type HashKey struct {
	key     []byte
	version int64
	fileID  []byte
}

func (hk *HashKey) HashSet(key, fileID, value []byte) (bool, error) {
	//先拿到元数据
	//构筑hash的key部分
	//对key进行编码

}

func (hk *HashKey) encode() {

}
