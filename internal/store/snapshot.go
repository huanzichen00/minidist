package store

import (
	"encoding/json"
	"os"
)

type snapshotData struct {
	Data              map[string]Value `json:"data"`
	MaxVersionCounter uint64           `json:"max_version_counter"`
}

func saveSnapshot(path string, snapshot snapshotData) error {
	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

func loadSnapshot(path string) (snapshotData, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return snapshotData{
			Data: make(map[string]Value),
		}, nil
	}
	if err != nil {
		return snapshotData{}, err
	}
	defer file.Close()

	var snapshot snapshotData
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil {
		return snapshotData{}, err
	}

	if snapshot.Data == nil {
		snapshot.Data = make(map[string]Value)
	}

	return snapshot, nil
}
