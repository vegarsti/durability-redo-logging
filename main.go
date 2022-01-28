package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Command is a struct used to deserialize a command from the log.
// It may be a SetCommand or a DeleteCommand.
// Since these use different keys, we can marshal into this catch-all
// struct and check which struct field was populated.
type Command struct {
	Set    []string
	Delete string
}

func (c Command) IsSetCommand() bool {
	return len(c.Set) == 2
}

func (c Command) IsDeleteCommand() bool {
	return len(c.Set) == 0
}

func (c Command) SetCommand() SetCommand {
	if !c.IsSetCommand() {
		panic("expected to be SetCommand")
	}
	return SetCommand{key: c.Set[0], value: c.Set[1]}
}

func (c Command) DeleteCommand() DeleteCommand {
	if !c.IsDeleteCommand() {
		panic("expected to be DeleteCommand")
	}
	return DeleteCommand{key: c.Delete}
}

type SetCommand struct {
	key   string
	value string
}

// Marshal the SetCommand into the JSON format used in the post.
// Hand-writing the JSON isn't idiomatic, but it works.
func (sc SetCommand) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"Set":["%s","%s"]}`, sc.key, sc.value)), nil
}

type DeleteCommand struct {
	key string
}

// Marshal the DeleteCommand into the JSON format used in the post.
// Hand-writing the JSON isn't idiomatic, but it works.
func (dc DeleteCommand) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"Delete":"%s"}`, dc.key)), nil
}

type DB struct {
	log      *os.File
	memtable map[string]string
	filename string
}

func NewDB(filename string) (*DB, error) {
	// Open file for appends (creates file if it doesn't exist)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("sync: %w", err)
	}
	db := &DB{
		log:      f,
		memtable: make(map[string]string),
		filename: filename,
	}
	if err := db.replayLog(); err != nil {
		return nil, fmt.Errorf("replayLog: %w", err)
	}
	return db, nil
}

func (d *DB) get(k string) string {
	return d.memtable[k]
}

func (d *DB) set(k string, v string) error {
	command := SetCommand{
		key:   k,
		value: v,
	}
	if err := d.applySetCommandToLog(command); err != nil {
		return fmt.Errorf("applySetCommandToLog: %w", err)
	}
	d.applySetCommandToMemtable(command)
	return nil
}

func (d *DB) delete(k string) error {
	command := DeleteCommand{key: k}
	if err := d.applyDeleteCommandToLog(command); err != nil {
		return fmt.Errorf("applyDeleteCommandToLog: %w", err)
	}
	d.applyDeleteCommandToMemtable(command)
	return nil
}

func (d *DB) applySetCommandToLog(command SetCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := d.log.Write(bs); err != nil {
		return fmt.Errorf("write command: %w", err)
	}
	if _, err := d.log.Write([]byte("\n")); err != nil {
		return fmt.Errorf("write newline: %w", err)
	}
	if err := d.log.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

func (d *DB) applyDeleteCommandToLog(command DeleteCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := d.log.Write(bs); err != nil {
		return fmt.Errorf("write command: %w", err)
	}
	if _, err := d.log.Write([]byte("\n")); err != nil {
		return fmt.Errorf("write newline: %w", err)
	}
	if err := d.log.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

func (d *DB) applySetCommandToMemtable(command SetCommand) {
	d.memtable[command.key] = command.value
}

func (d *DB) applyDeleteCommandToMemtable(command DeleteCommand) {
	delete(d.memtable, command.key)
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
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
		if cmd.IsSetCommand() {
			d.applySetCommandToMemtable(cmd.SetCommand())
		}
		if cmd.IsDeleteCommand() {
			d.applyDeleteCommandToMemtable(cmd.DeleteCommand())
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
