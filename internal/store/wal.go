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

	// 低层日志文件
	// WAL 所有记录都追加到这个文件结尾
	file *os.File
}

// ReplayStats 记录 WAL 回放结果。
type ReplayStats struct {
	Records      int
	TailRepaired bool
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

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if info.Size() > 0 && info.Size() < walHeaderSize {
		_ = file.Close()
		return nil, fmt.Errorf("wal header is incomplete")
	}

	if info.Size() == 0 {
		if err := writeWALHeader(file); err != nil {
			_ = file.Close()
			return nil, err
		}
	} else if err := validateWALHeader(file); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

// Append 向 WAL 追加一条带校验和的记录。
func (w *WAL) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if uint64(len(data)) > uint64(maxWALRecordSize) {
		return fmt.Errorf("wal record too large: %d bytes", len(data))
	}

	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(data))
	if _, err := w.file.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(data); err != nil {
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

	validOffset := int64(walHeaderSize)

	if _, err := w.file.Seek(validOffset, io.SeekStart); err != nil {
		return stats, err
	}

	for {
		var header [8]byte
		_, err := io.ReadFull(w.file, header[:])
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

		length := binary.BigEndian.Uint32(header[0:4])
		if length > maxWALRecordSize {
			return stats, fmt.Errorf("wal record too large: %d bytes", length)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			if err == io.ErrUnexpectedEOF {
				if err := w.repairTail(validOffset); err != nil {
					return stats, err
				}

				stats.TailRepaired = true
				return stats, nil
			}
			return stats, err
		}

		expectedChecksum := binary.BigEndian.Uint32(header[4:8])
		if actualChecksum := crc32.ChecksumIEEE(data); actualChecksum != expectedChecksum {
			return stats, fmt.Errorf("wal checksum mismatch: expected %08x, got %08x", expectedChecksum, actualChecksum)
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

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(walHeaderSize); err != nil {
		return err
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}

	size := info.Size() - walHeaderSize
	if size < 0 {
		return 0, nil
	}

	return size, nil
}

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

func writeWALHeader(file *os.File) error {
	var header [walHeaderSize]byte

	binary.BigEndian.PutUint32(header[0:4], walMagic)
	binary.BigEndian.PutUint16(header[4:6], walVersion)

	if _, err := file.Write(header[:]); err != nil {
		return err
	}

	return file.Sync()
}

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
