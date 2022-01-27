package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type SetCommand struct {
	key   string
	value string
}

func (sc SetCommand) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"Set":["%s","%s"]}`, sc.key, sc.value)), nil
}

type DeleteCommand struct {
	key string
}

func (dc DeleteCommand) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"Delete":"%s"}`, dc.key)), nil
}

type DB struct {
	log      io.Writer
	filename string
}

func NewDB(filename string) (*DB, error) {
	f, err := os.Open(filename)
	if os.IsNotExist(err) {
		f, err := os.Create(filename)
		if err != nil {
			return nil, fmt.Errorf("create: %w", err)
		}
		return &DB{
			log:      f,
			filename: filename,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return &DB{
		log:      f,
		filename: filename,
	}, nil
}

func (d *DB) set(k string, v string) {
	command := SetCommand{
		key:   k,
		value: v,
	}
	d.applySetCommand(command)
}

func (d *DB) delete(k string) {
	command := DeleteCommand{
		key: k,
	}
	d.applyDeleteCommand(command)
}

func (d *DB) applySetCommand(command SetCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	d.log.Write(bs)
	d.log.Write([]byte("\n"))
	return nil
}

func (d *DB) applyDeleteCommand(command DeleteCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	d.log.Write(bs)
	d.log.Write([]byte("\n"))
	return nil
}

func main() {
	db, err := NewDB("db_data")
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewDB: %v\n", err)
		os.Exit(1)
	}
	db.set("foo", "a")
	db.set("bar", "b")
	db.set("baz", "c")
	db.delete("bar")
}
