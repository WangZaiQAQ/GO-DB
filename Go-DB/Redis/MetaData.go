package Redis

import "encoding/binary"

const MaxMetaDataSize =1+binary.MaxVarintLen64*2

type metadata struct {
	DataType byte
	expire   int64
	versin   int64
	siz      uint32
	head     uint64
	tail     uint64
}

func (md *metadata) encodeMetaData {
	var size =MaxMetaDataSize
}

func (md *metadata)FindMetaData{


}