package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// ErrNotFound 表示指定 chunk 不存在。
var ErrNotFound = errors.New("chunk not found")

// Store 是按内容寻址保存 chunk 的本地存储。
type Store struct {
	root string
}

// Open 打开本地 chunk store，并确保根目录存在。
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("chunk root is empty")
	}

	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}

	return &Store{
		root: root,
	}, nil
}

// ID 根据 chunk 内容计算稳定的 SHA-256 标识。
func ID(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Put 按内容寻址方式将 chunk 写入磁盘，并返回其 ID。
func (s *Store) Put(data []byte) (string, error) {
	id := ID(data)
	path := s.path(id)

	if existing, err := os.ReadFile(path); err == nil {
		if ID(existing) == id {
			return id, nil
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	// 每次写入使用独立临时文件，避免相同 chunk 并发写入时争用固定 .tmp 文件。
	file, err := os.CreateTemp(dir, ".chunk-*.tmp")
	if err != nil {
		return "", err
	}
	tmpPath := file.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return "", err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", err
	}

	if err := file.Close(); err != nil {
		return "", err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return "", err
	}

	return id, nil
}

// Get 读取指定 chunk，并校验内容是否与 chunk ID 匹配。
func (s *Store) Get(id string) ([]byte, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}

	path := s.path(id)

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if ID(data) != id {
		return nil, fmt.Errorf("chunk checksum mismatch: %s", id)
	}

	return data, nil
}

// Delete 删除指定 chunk；chunk 不存在时视为成功。
func (s *Store) Delete(id string) error {
	if err := validateID(id); err != nil {
		return err
	}

	err := os.Remove(s.path(id))
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

// Exists 判断指定 chunk 是否已经存在。
func (s *Store) Exists(id string) (bool, error) {
	if err := validateID(id); err != nil {
		return false, err
	}

	_, err := os.Stat(s.path(id))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

// path 把 chunk ID 映射为本地文件路径
func (s *Store) path(id string) string {
	return filepath.Join(s.root, id[0:2], id[2:4], id)
}

// validateID 校验 chunk ID 是否是合法 SHA-256 十六进制字符串
func validateID(id string) error {
	if len(id) != sha256.Size*2 {
		return fmt.Errorf("invalid chunk id length: %d", len(id))
	}

	if _, err := hex.DecodeString(id); err != nil {
		return fmt.Errorf("invalid chunk id: %w", err)
	}

	return nil
}

// IDs 返回当前节点底层保存的所有合法 chunk ID
func (s *Store) IDs() ([]string, error) {
	var ids []string

	err := filepath.WalkDir(s.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		if !entry.Type().IsRegular() {
			return nil
		}

		id := entry.Name()
		if err := validateID(id); err != nil {
			return nil
		}

		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(ids)
	return ids, nil
}
