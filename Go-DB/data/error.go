package data

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdataFailed = errors.New("failed to updara index")
	ErrKeyIsNotFound     = errors.New("该key不存在")
	ErrExceeMaxBatchNum  = errors.New("事务提交的操作数超过最大允许数")
	ErrDataFileNotFound  = errors.New("该数据文件不存在")
	ErrDataDirCorrupted  = errors.New("数据库的目录可能已被损坏")
	ErrMergeIsProgress   = errors.New("Merge正在进行中，请稍后再尝试")
)
