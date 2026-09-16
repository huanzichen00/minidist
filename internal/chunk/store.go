package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

	if _, err := os.Stat(path); err == nil {
		return id, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", err
	}

	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}

	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
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
