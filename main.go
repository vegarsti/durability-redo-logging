package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
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
	filename string
}

func NewDB(filename string) (*DB, error) {
	// Open file for append writes, create file if it doesn't exist
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	f.Sync()
	return &DB{
		log:      f,
		filename: filename,
	}, nil
}

func (d *DB) get(k string) (string, error) {
	f, err := os.Open(d.filename)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	result := ""
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		x := scanner.Bytes()
		var cmd Command
		json.Unmarshal(x, &cmd)
		if len(cmd.Set) == 2 { // is Set command
			if k == cmd.Set[0] {
				result = cmd.Set[1]
			}
		} else { // is Delete
			if k == cmd.Delete {
				result = ""
			}
		}
	}
	return result, nil
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
	d.log.Sync()
	return nil
}

func (d *DB) applyDeleteCommand(command DeleteCommand) error {
	bs, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	d.log.Write(bs)
	d.log.Write([]byte("\n"))
	d.log.Sync()
	return nil
}

func main() {
	db, err := NewDB("db_data")
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewDB: %v\n", err)
		os.Exit(1)
	}
	// Get before writes
	foo, err := db.get("foo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("foo = '%s'\n", foo)
	bar, err := db.get("bar")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("bar = '%s'\n", bar)
	baz, err := db.get("baz")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("baz = '%s'\n", baz)

	// Write
	fmt.Println("Performing writes...")
	db.set("foo", "a")
	db.set("bar", "b")
	db.set("baz", "c")
	db.delete("bar")

	// Get after writes
	foo, err = db.get("foo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("foo = '%s'\n", foo)
	bar, err = db.get("bar")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("bar = '%s'\n", bar)
	baz, err = db.get("baz")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("baz = '%s'\n", baz)
}
