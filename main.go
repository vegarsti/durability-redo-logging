package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type DB struct {
	data     map[string]string
	filename string
}

func NewDB(filename string) (*DB, error) {
	bs, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return &DB{
			data:     make(map[string]string),
			filename: filename,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	data := make(map[string]string)
	if err := json.Unmarshal(bs, &data); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &DB{
		data:     data,
		filename: filename,
	}, nil
}

func (d *DB) get(k string) string {
	return d.data[k]
}

func (d *DB) set(k string, v string) {
	d.data[k] = v
}

func (d *DB) delete(k string) {
	delete(d.data, k)
}

func (d *DB) flush() error {
	bs, err := json.Marshal(d.data)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(d.filename, bs, 0644); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func main() {
	db, err := NewDB("db_data")
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewDB: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Value of abc is '%s'\n", db.get("abc"))
	db.set("abc", "def")
	fmt.Printf("Value of abc is '%s'\n", db.get("abc"))
	db.flush()
}
