package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Command struct {
	Set    []string
	Delete string
}

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
	log      *os.File
	memtable map[string]string
	filename string
}

func NewDB(filename string) (*DB, error) {
	// Open file for append writes, create file if it doesn't exist
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	// f.Sync()
	db := &DB{
		log:      f,
		memtable: make(map[string]string),
		filename: filename,
	}
	db.replayLog()
	return db, nil
}

func (d *DB) get(k string) (string, error) {
	return d.memtable[k], nil
}

func (d *DB) set(k string, v string) {
	command := SetCommand{
		key:   k,
		value: v,
	}
	d.applySetCommand(command)
	d.applySetCommandToMemtable(command)
}

func (d *DB) delete(k string) {
	command := DeleteCommand{
		key: k,
	}
	d.applyDeleteCommand(command)
	d.applyDeleteCommandToMemtable(command)
}

func (d *DB) applySetCommand(command SetCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	d.log.Write(bs)
	d.log.Write([]byte("\n"))
	// d.log.Sync()
	return nil
}

func (d *DB) applyDeleteCommand(command DeleteCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	d.log.Write(bs)
	d.log.Write([]byte("\n"))
	// d.log.Sync()
	return nil
}

func (d *DB) applySetCommandToMemtable(command SetCommand) error {
	d.memtable[command.key] = command.value
	return nil
}

func (d *DB) applyDeleteCommandToMemtable(command DeleteCommand) error {
	delete(d.memtable, command.key)
	return nil
}

func (d *DB) replayLog() error {
	f, err := os.Open(d.filename)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var cmd Command
		json.Unmarshal(scanner.Bytes(), &cmd)
		if len(cmd.Set) == 2 { // is Set command
			k := cmd.Set[0]
			v := cmd.Set[1]
			d.applySetCommandToMemtable(SetCommand{key: k, value: v})
		} else { // is Delete
			k := cmd.Delete
			d.applyDeleteCommandToMemtable(DeleteCommand{key: k})
		}
	}
	return nil
}

func main() {
	db, err := NewDB("logfile")
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewDB: %v\n", err)
		os.Exit(1)
	}
	t := time.Now()
	writes := 0
	for time.Since(t).Milliseconds() < 10000 {
		writes++
		db.set("foo", "bar")
	}
	fmt.Printf("performed %d writes in 10 seconds\n", writes)
}
