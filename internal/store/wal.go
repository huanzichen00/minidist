package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	walMagic      uint32 = 0x4D44574C // "MDWL"
	walVersion    uint16 = 1
	walHeaderSize        = 8

	maxWALRecordSize uint32 = 16 * 1024 * 1024
)

// Write-Ahead Log
type WAL struct {
	mu sync.Mutex

	// 底层日志文件
	// WAL 所有记录都追加到这个文件结尾
	file *os.File

	hasHeader bool
}

type ReplayStats struct {
	Records      int
	TailRepaired bool
}

// OpenWAL 打开或创建指定路径的预写日志文件，并校验其文件头。
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

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	hasHeader, err := detectWALHeader(file, info.Size())
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		if err := writeWALHeader(file); err != nil {
			_ = file.Close()
			return nil, err
		}
		hasHeader = true
	}

	return &WAL{
		file:      file,
		hasHeader: hasHeader,
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

	if uint64(len(data)) > uint64(maxWALRecordSize) {
		return fmt.Errorf("wal record too large: %d bytes", len(data))
	}

	if err := w.writeRecord(data); err != nil {
		return err
	}

	// sync 稳定存储
	return w.file.Sync()
}

// Replay 从头扫描 WAL，把每条完整记录交给 apply 处理
// 用于进程启动时恢复内存状态
func (w *WAL) Replay(apply func([]byte) error) (ReplayStats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var stats ReplayStats

	validOffset := int64(0)
	if w.hasHeader {
		validOffset = walHeaderSize
	}

	if _, err := w.file.Seek(validOffset, io.SeekStart); err != nil {
		return stats, err
	}

	for {
		data, err := w.readRecord()
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if err := w.repairTail(validOffset); err != nil {
				return stats, err
			}

			stats.TailRepaired = true
			return stats, nil
		}
		if err != nil {
			return stats, err
		}

		if err := apply(data); err != nil {
			return stats, err
		}

		stats.Records++

		offset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return stats, err
		}

		validOffset = offset
	}

	// 文件偏移移到末尾
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return stats, err
	}

	return stats, nil
}

// Close 关闭 WAL 底层文件。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

// Truncate 清空 WAL，并重新写入当前版本的文件头。
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := writeWALHeader(w.file); err != nil {
		return err
	}
	w.hasHeader = true

	return w.file.Sync()
}

// Size 返回 WAL 记录区占用的字节数，不包含文件头。
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}

	size := info.Size()
	if w.hasHeader {
		size -= walHeaderSize
	}
	if size < 0 {
		return 0, nil
	}

	return size, nil
}

// repairTail 将损坏或不完整的日志尾部截断到最后一个有效偏移。
func (w *WAL) repairTail(validOffset int64) error {
	if err := w.file.Truncate(validOffset); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

// writeWALHeader 向文件起始位置写入 WAL 文件头。
func writeWALHeader(file *os.File) error {
	var header [walHeaderSize]byte

	binary.BigEndian.PutUint32(header[0:4], walMagic)
	binary.BigEndian.PutUint16(header[4:6], walVersion)

	if _, err := file.Write(header[:]); err != nil {
		return err
	}

	return file.Sync()
}

// detectWALHeader 检查文件是否包含受支持的 WAL 文件头。
func detectWALHeader(file *os.File, size int64) (bool, error) {
	if size == 0 {
		return false, nil
	}
	if size < walHeaderSize {
		return false, fmt.Errorf("wal header is incomplete")
	}

	var magicBytes [4]byte
	if _, err := file.ReadAt(magicBytes[:], 0); err != nil {
		return false, err
	}

	if binary.BigEndian.Uint32(magicBytes[:]) != walMagic {
		return false, nil
	}

	if size < walHeaderSize {
		return false, fmt.Errorf("wal header is incomplete")
	}

	return true, validateWALHeader(file)
}

// validateWALHeader 校验 WAL 文件头中的魔数和版本。
func validateWALHeader(file *os.File) error {
	var header [walHeaderSize]byte

	if _, err := file.ReadAt(header[:], 0); err != nil {
		return err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != walMagic {
		return fmt.Errorf("invalid wal magic: got %08x", magic)
	}

	version := binary.BigEndian.Uint16(header[4:6])
	if version != walVersion {
		return fmt.Errorf("unsupported wal version: %d", version)
	}

	return nil
}

// writeRecord 将一条数据记录编码并追加到当前文件偏移处。
func (w *WAL) writeRecord(data []byte) error {
	if w.hasHeader {
		var header [8]byte
		binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
		binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(data))
		if _, err := w.file.Write(header[:]); err != nil {
			return err
		}
	} else {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(data)))
		if _, err := w.file.Write(header[:]); err != nil {
			return err
		}
	}

	_, err := w.file.Write(data)
	return err
}

// readRecord 从当前文件偏移读取并校验一条完整记录。
func (w *WAL) readRecord() ([]byte, error) {
	headerSize := 4
	if w.hasHeader {
		headerSize = 8
	}

	header := make([]byte, headerSize)
	if _, err := io.ReadFull(w.file, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	if length > maxWALRecordSize {
		return nil, fmt.Errorf("wal record too large: %d bytes", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(w.file, data); err != nil {
		return nil, err
	}

	if w.hasHeader {
		expectedChecksum := binary.BigEndian.Uint32(header[4:8])
		actualChecksum := crc32.ChecksumIEEE(data)
		if actualChecksum != expectedChecksum {
			return nil, fmt.Errorf("wal checksum mismatch: expected %08x, got %08x",
				expectedChecksum, actualChecksum)
		}
	}

	return data, nil
}
