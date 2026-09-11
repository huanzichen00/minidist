package store

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

// Write-Ahead Log
type WAL struct {
	mu sync.Mutex

	// 低层日志文件
	// WAL 所有记录都追加到这个文件结尾
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	// O_CREATE:
	//   文件不存在时自动创建。
	//
	// O_RDWR:
	//   既要写 WAL，也要在启动恢复时读取 WAL。
	//
	// O_APPEND:
	//   每次 Write 都追加到文件末尾，
	//   避免覆盖之前已经写入的日志记录。
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

// Append 向 WAL 中追加一条记录
// 格式:
// 4 bytes length
// N bytes payload
// e.g. [00 00 00 05][hello]
//
//	       ^
//	       |
//	payload 长度为 5
func (w *WAL) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// uint32 最大可表示约 4GB
	var size [4]byte
	// BigEndian 把 uint32 编码成 4 个字节
	binary.BigEndian.PutUint32(size[:], uint32(len(data)))

	// 写长度
	if _, err := w.file.Write(size[:]); err != nil {
		return err
	}

	// 写 payload
	if _, err := w.file.Write(data); err != nil {
		return err
	}

	// sync 稳定存储
	return w.file.Sync()
}

// Replay 从头扫描 WAL，把每条完整记录交给 apply 处理
// 用于进程启动时恢复内存状态
func (w *WAL) Replay(apply func([]byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 指针移到文件开头
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		var size [4]byte

		// ReadFull 保证读满 4 字节或返回错误
		_, err := io.ReadFull(w.file, size[:])
		if err == io.EOF {
			break
		}

		// 文件最后连 4 字节长度字段都没写完整
		// 先忽略
		if err == io.ErrUnexpectedEOF {
			break
		}

		if err != nil {
			return err
		}

		// 解码回 uint32
		length := binary.BigEndian.Uint32(size[:])
		data := make([]byte, length)

		// 长度字段写成功但 payload 写了一部分就崩溃
		_, err = io.ReadFull(w.file, data)
		if err == io.ErrUnexpectedEOF {
			break
		}

		if err != nil {
			return err
		}

		// 解释数据由上层实现
		// WAL 不解析 payload
		if err := apply(data); err != nil {
			return err
		}
	}

	// 文件偏移移到末尾
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}
